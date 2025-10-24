# build_sql_job.py (v5) — outputs full SQL, full JSON, full audit markdown
import os, re, json
import pandas as pd
from collections import Counter
from typing import List, Dict, Any, Tuple

from rule_utils import (
    squash, clean_free_text, parse_literal_set, transformation_expression,
    normalize_join, business_rules_to_where, detect_lookup, parse_set_rule, _debug_log
)

# ---------- CSV loading & column mapping ----------

def _rename_dupe_headers(df: pd.DataFrame) -> pd.DataFrame:
    rename_map, seen = {}, {}
    for col in df.columns:
        if col not in seen:
            seen[col] = 0
            rename_map[col] = col
        else:
            seen[col] += 1
            rename_map[col] = f"{col}__{seen[col]}"
    return df.rename(columns=rename_map)

def load_mapping(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, engine="python")
    df = _rename_dupe_headers(df)

    # Map your known headers to canonical names
    colmap = {
        "Table/File Name * (auto populate)": "src_table",
        "Column Name * (auto populate)": "src_column",
        "Table/File Name * (auto populate)__1": "tgt_table",
        "Column/Field Name * (auto populate)": "tgt_column",
        "Data Type * (auto populate)__1": "tgt_datatype",
        "Business Rule (auto populate)": "business_rule",
        "Join Clause (auto populate)": "join_clause",
        "Transformation Rule/Logic (auto populate)": "transformation_rule",
        "DB Name/Outgoing File Path * (auto populate)": "tgt_path",
        "DB Name/Incoming File Path *": "src_path",
    }
    for k, v in colmap.items():
        if k in df.columns:
            df[v] = df[k]

    # keep blanks as empty strings (not NaN)
    return df.fillna("")

# ---------- Inference ----------

def infer_sources(df: pd.DataFrame) -> List[str]:
    return sorted(set([str(s).strip() for s in df.get("src_table", pd.Series()) if str(s).strip()]))

def infer_target(df: pd.DataFrame) -> str:
    vals = [str(t).strip() for t in df.get("tgt_table", pd.Series()) if str(t).strip()]
    return Counter(vals).most_common(1)[0][0] if vals else "target_table"

def choose_primary(df: pd.DataFrame) -> str:
    counts = Counter([str(r) for r in df.get("src_table", pd.Series()) if r])
    return counts.most_common(1)[0][0] if counts else "source_table mas"

# ---------- SQL builders ----------

def build_cte_sources(sources: List[str]) -> Tuple[List[str], List[str]]:
    ctes = []
    seen_aliases = set()
    aliases_out = []
    for s in sources:
        parts = s.split()
        if len(parts) >= 2:
            table, alias = parts[0], parts[-1]
        else:
            table = s
            alias = s.split("_")[0] if "_" in s else "src"
        base = re.sub(r"[^A-Za-z0-9_]", "", alias) or "src"
        alias_u = base
        i = 1
        while alias_u in seen_aliases:
            alias_u = f"{base}{i}"
            i += 1
        seen_aliases.add(alias_u)
        aliases_out.append(alias_u)
        ctes.append(f"{alias_u} AS (SELECT * FROM {table} {alias_u})")
    return ctes, aliases_out

def build_step1_cte(df: pd.DataFrame, primary_src: str) -> str:
    """
    Build the base CTE with clean deduplicated JOINs and WHERE clauses.
    Ensures LEFT JOIN safety and removes repeated or identical business rules.
    """
    base_alias = (
        primary_src.split()[-1]
        if " " in primary_src
        else (primary_src.split("_")[0] if "_" in primary_src else "mas")
    )

    # ----- JOIN normalization -----
    join_texts = df.get("join_clause", pd.Series()).tolist()
    normalized_joins = []
    seen_joins = set()
    for txt in join_texts:
        j = normalize_join(txt)
        if not j:
            continue
        # Remove accidental self-joins and duplicate entries (case-insensitive)
        if re.search(fr"\b{base_alias}\b", j, re.I) and re.search(fr"\b{primary_src}\b", j, re.I):
            continue
        key = j.lower().strip()
        if key not in seen_joins:
            normalized_joins.append(j)
            seen_joins.add(key)

    joins = [f"  {j}" for j in normalized_joins]
    join_clause = "\n".join(joins)

    # ----- Business rules normalization -----
    br_blocks_raw = [business_rules_to_where(txt) for txt in df.get("business_rule", pd.Series()).tolist()]
    # Deduplicate business rules based on their cleaned body
    seen_rules = set()
    br_blocks = []
    for blk in br_blocks_raw:
        key = blk.lower().strip()
        if key and key not in seen_rules:
            br_blocks.append(blk)
            seen_rules.add(key)

    where_lines = []
    for idx, blk in enumerate(br_blocks, 1):
        where_lines.append(f"-- Business Rule Block #{idx}\n  {blk}")

    where_clause = (
        "\nWHERE\n  " + "\n  AND ".join([l for l in where_lines if l])
        if where_lines
        else ""
    )

    return (
        "step1 AS (\n"
        f"  SELECT {base_alias}.*\n"
        f"  FROM {primary_src} {base_alias}\n"
        f"{join_clause}\n"
        f"{where_clause}\n)"
    )

def build_final_select(df: pd.DataFrame) -> Tuple[str, List[Dict[str, str]]]:
    """
    Return SELECT body string plus an audit list of {"row", "target", "raw", "sql", "note"}.
    Handles duplicate target columns by merging transformation rules.
    """
    lines = []
    audit_rows = []

    # Group by target column (case-insensitive)
    grouped = df.groupby(df["tgt_column"].str.lower(), dropna=False)

    for tgt_lower, group in grouped:
        tgt = group["tgt_column"].iloc[0]
        # Combine multiple rows for same target
        unique_rules = list({r.strip() for r in group.get("transformation_rule", []) if str(r).strip()})
        unique_sources = list({s.strip() for s in group.get("src_column", []) if str(s).strip()})
        merged_note = ""

        # Pick first transformation logic
        if len(unique_rules) > 1:
            merged_note = f"-- NOTE: merged {len(unique_rules)} variations for target column '{tgt}'"
        elif len(group) > 1:
            merged_note = f"-- NOTE: merged {len(group)} duplicate definitions for target column '{tgt}'"

        raw_trans = unique_rules[0] if unique_rules else ""
        src_col = unique_sources[0] if unique_sources else ""

        expr, trailing_comment = transformation_expression(
            raw_trans, target_col=tgt, src_col=src_col
        )

        # 🟢 Expand placeholders (e.g. {source_column}) from parse_set_rule results
        if "{source_column}" in expr:
            expr = expr.replace("{source_column}", src_col or "NULL")

        # 🩹 Fix 1: remove stray quotes wrapping CASE/SELECT expressions
        if re.match(r"^['\"]\s*(CASE|SELECT)\b", expr, re.I):
            old_expr = expr
            expr = re.sub(r"^['\"]|['\"]$", "", expr.strip())
            if DEBUG_TRANSFORMATIONS:
                _debug_log(
                    "TRANSFORMATION (dequote fix)",
                    f"Target: {tgt}\nBefore: {old_expr}\nAfter: {expr}"
                )

        # 🩹 Fix 2: remove trailing semicolon (if any)
        expr = re.sub(r";+$", "", expr).strip()

        # Build SQL select line
        select_line = f"    {expr} AS {tgt}"
        if merged_note:
            select_line = f"    {merged_note}\n{select_line}"
        if trailing_comment:
            select_line = f"{select_line}\n    {trailing_comment}"

        lines.append(select_line)

        # Audit tracking
        audit_rows.append({
            "row": f"{group.index.min() + 1}",
            "target": tgt,
            "raw": " | ".join(unique_rules) or "",
            "sql": expr.strip(),
            "note": merged_note or (trailing_comment or "")
        })

    return "SELECT\n" + ",\n".join(lines) + "\nFROM step1", audit_rows

def build_sql_cte_pipeline(df: pd.DataFrame, target_table: str) -> Tuple[str, List[Dict[str, str]]]:
    sources = infer_sources(df)
    cte_sources, _aliases = build_cte_sources(sources)
    primary = choose_primary(df)
    step1 = build_step1_cte(df, primary_src=primary)
    final_select, audit_rows = build_final_select(df)
    sql_text = "WITH\n" + ",\n".join(cte_sources + [step1]) + "\n" + final_select + ";\n"
    return sql_text, audit_rows

# ---------- Job JSON builder ----------

def build_job_json(source_malcode: str, target_table: str, sql_path: str, df: pd.DataFrame) -> Dict[str, Any]:
    sources = infer_sources(df)
    sourcelist = [s.split()[0] for s in sources] if sources else []

    modules = {}

    dsp = {
        "options": {"module": "data_sourcing_process", "method": "process"},
        "loggable": True,
        "sourcelist": sourcelist
    }
    for s in sources:
        table = s.split()[0]
        dsp[table] = {
            "type": "sz_zone",
            "table.name": table,
            "read-format": "view",
            "path": "${adls.source.root}/" + table
        }
    modules["data_sourcing_process"] = dsp

    text_cols = ["join_clause", "business_rule", "transformation_rule"]
    blob = " ".join([str(df.get(c, pd.Series()).to_string()) for c in text_cols])
    if detect_lookup([blob]):
        modules["lookup_cd"] = {
            "sql": (
                "SELECT source_value1, stndrd_cd_value, stndrd_cd_name\n"
                "FROM delta.'${adls.lookup.path}/AE_standard_code_mapping_rules'\n"
                f"WHERE source_cd = '{source_malcode}'"
            ),
            "loggable": True,
            "options": {"module": "data_transformation", "method": "process"},
            "name": "lookup_cd"
        }

    modules["data_transformation"] = {
        "sql": f"@{sql_path}",
        "loggable": True,
        "options": {"module": "data_transformation", "method": "process"},
        "name": f"dt_{target_table}"
    }

    modules["load_enrich_process"] = {
        "options": {"module": "load_enrich_process", "method": "process"},
        "loggable": True,
        "sql": f"SELECT * FROM dt_{target_table}",
        "target-path": "${adls.stage.root}/${source malcode}",
        "mode-of-write": "replace_partition",
        "keys": "",
        "cdc-flag": False,
        "scd2-flag": False,
        "partition-by": "effective_dt",
        "target-format": "delta",
        "target-table": f"/{target_table}",
        "name": f"{target_table}_daily"
    }

    return {
        "source malcode": source_malcode,
        "source basepath": source_malcode.upper(),
        "comment": f"Auto-generated job for {target_table} from mapping CSV.",
        "modules": modules
    }

# ---------- Audit markdown ----------

def write_audit_md(audit_rows: List[Dict[str, str]], md_path: str):
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# Transformation Rules Audit\n\n")
        f.write("| Row | Target Column | Raw Transformation (verbatim) | Parsed SQL Expression | Notes |\n")
        f.write("|---:|---|---|---|---|\n")
        for r in audit_rows:
            # escape | in markdown table safely and silence invalid escape warnings
            raw = (r["raw"] or "").replace("\n", "<br>").replace("|", "\\|")
            sql = (r["sql"] or "").replace("\n", "<br>").replace("|", "\\|")
            note = (r["note"] or "").replace("\n", "<br>").replace("|", "\\|")
            f.write(f"| {r['row']} | `{r['target']}` | {raw} | `{sql}` | {note} |\n")

# ---------- Orchestration ----------

def generate(csv_path: str, outdir: str, source_malcode: str = "ND") -> Dict[str, str]:
    df = load_mapping(csv_path)
    target = infer_target(df)
    os.makedirs(outdir, exist_ok=True)
    job_dir = os.path.join(outdir, f"{target.lower()}_job")
    os.makedirs(job_dir, exist_ok=True)

    # Build SQL & audit
    sql_text, audit_rows = build_sql_cte_pipeline(df, target)
    sql_path = os.path.join(job_dir, f"{target.lower()}_pipeline.sql")
    with open(sql_path, "w", encoding="utf-8") as f:
        f.write(sql_text)

    # Build JSON
    job_json = build_job_json(source_malcode, target, sql_path, df)
    json_path = os.path.join(job_dir, f"{target.lower()}_job.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(job_json, f, indent=2)

    # Build audit markdown
    md_path = os.path.join(job_dir, "transformation_rules_audit.md")
    write_audit_md(audit_rows, md_path)

    return {"target": target, "sql_path": sql_path, "json_path": json_path, "audit_path": md_path}

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--outdir", default="generated_jobs_full_run_v1")
    ap.add_argument("--source_malcode", default="ND")
    args = ap.parse_args()
    res = generate(args.csv, args.outdir, args.source_malcode)
    print(json.dumps(res, indent=2))
