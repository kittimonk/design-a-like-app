# build_sql_job_merged_v1_1.py
# Based on your validated build_sql_job_merged.py (v14)
# Changes are minimal and fully backward compatible.

import os, re, json
import pandas as pd
from collections import Counter
from typing import List, Dict, Any, Tuple

from rule_utils_merged_v2 import (
    squash, clean_free_text, parse_literal_set, transformation_expression,
    normalize_join, business_rules_to_where, detect_lookup, parse_set_rule,
    _infer_datatype_from_value, _cast_to_datatype, _debug_log, _needs_cast,
    _guard_suspicious, _ensure_unique_join_aliases
)

DEBUG_TRANSFORMATIONS = True
DEBUG_JOINS = True
DEBUG_OUTPUT_DIR = "debug_outputs"

# ============================================================
# [PATCH v1.1-A] Helpers for alias & join dedupe enhancements
# ============================================================

_JOIN_RX = re.compile(
    r"(?i)^\s*(left|inner|right|full)\s+join\s+([A-Za-z0-9_]+)(?:\s+([A-Za-z0-9_]+))?\s+on\s+(.+)$"
)

def _ensure_unique_join_aliases(joins: list, base_alias: str = "mas") -> list:
    """Ensure each JOIN alias is unique (ref, ref1, ref2, etc.), dedupe identical joins."""
    used = {base_alias.lower()}
    out, seen = [], set()
    for j in joins:
        if not j or not isinstance(j, str):
            continue
        m = _JOIN_RX.match(j.strip())
        if not m:
            sig = re.sub(r"\s+", " ", j.strip().lower())
            if sig not in seen:
                seen.add(sig)
                out.append(j)
            continue
        _, table, alias, on = m.groups()
        if not alias or alias.lower() in used:
            alias_base = "ref" if table.lower().startswith("gls") else table.lower()[:3]
            k = 1
            alias2 = alias_base
            while alias2.lower() in used:
                alias2 = f"{alias_base}{k}"
                k += 1
            alias = alias2
        used.add(alias.lower())
        on_norm = re.sub(r"\s+", " ", on).strip()
        js = f"LEFT JOIN {table} {alias} ON {on_norm}"
        sig = re.sub(r"\s+", " ", js.lower())
        if sig not in seen:
            seen.add(sig)
            out.append(js)
    return out

_SUSPICIOUS = re.compile(r"(?i)string_agg\s*\(\s*format\s*\(\s*ascii\s*\(", re.DOTALL)
def _guard_suspicious(expr: str) -> str:
    """Guard unresolved function fragments (like STRING_AGG(FORMAT(ASCII...)) so SQL compiles."""
    if not isinstance(expr, str):
        return expr
    if _SUSPICIOUS.search(expr or ""):
        short = (expr[:120] + "...") if len(expr) > 120 else expr
        short = short.replace("/*", "/ *").replace("*/", "* /")
        return f"NULL /* unresolved expression guarded: {short} */"
    return expr


# ============================================================
# ---------- CSV loading & column mapping ----------
# ============================================================

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

def _write_debug(name: str, content: str):
    """Write debug logs to file."""
    os.makedirs(DEBUG_OUTPUT_DIR, exist_ok=True)
    path = os.path.join(DEBUG_OUTPUT_DIR, name)
    with open(path, "a", encoding="utf-8") as f:
        f.write(content.strip() + "\n\n")

def load_mapping(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, engine="python")
    df = _rename_dupe_headers(df)
    colmap = {
        "Table/File Name * (auto populate)": "src_table",
        "Column Name * (auto populate)": "src_column",
        "Table/File Name * (auto populate)__1": "tgt_table",
        "Table/File Name * (auto populate).1": "tgt_table",
        "Column/Field Name * (auto populate)": "tgt_column",
        "Data Type * (auto populate)__1": "tgt_datatype",
        "Data Type * (auto populate).1": "tgt_datatype",
        "Business Rule (auto populate)": "business_rule",
        "Join Clause (auto populate)": "join_clause",
        "Transformation Rule/Logic (auto populate)": "transformation_rule",
        "DB Name/Outgoing File Path * (auto populate)": "tgt_path",
        "DB Name/Incoming File Path *": "src_path",
    }
    for k, v in colmap.items():
        if k in df.columns:
            df[v] = df[k]

    # 🧩 Auto-clean JOIN fragments inside transformation_rule
    if "transformation_rule" in df.columns and "join_clause" in df.columns:
        for i, row in df.iterrows():
            tr = str(row.get("transformation_rule", "")).strip()
            jc = str(row.get("join_clause", "")).strip()
            if re.search(r"(?i)\b(join|from|on)\b", tr):
                join_part_match = re.search(r"(?i)\b(from|join|on)\b.*", tr, re.DOTALL)
                if join_part_match:
                    join_part = join_part_match.group(0)
                    new_jc = jc + " " + join_part if jc else join_part
                    df.at[i, "join_clause"] = new_jc.strip()
                    tr_clean = re.sub(r"(?i)\b(from|join|on)\b.*", "", tr, flags=re.DOTALL).strip()
                    df.at[i, "transformation_rule"] = tr_clean
                    _write_debug(
                        "auto_join_cleanup.log",
                        f"Moved JOIN/FROM from transformation_rule[{i}] to join_clause:\n"
                        f"  OLD: {tr}\n  NEW join_clause: {new_jc}\n"
                    )
    return df.fillna("")


# ============================================================
# ---------- CTE Builders ----------
# ============================================================

def _sanitize_alias_leaks(join_sql: str, base_alias: str, known_aliases: List[str]) -> str:
    if not join_sql:
        return join_sql
    out = join_sql
    for alias in known_aliases:
        pattern = rf"(?<![\w]){re.escape(alias)}\."
        out = re.sub(pattern, f"{base_alias}.", out, flags=re.I)
    return out


def build_step1_cte(df: pd.DataFrame, primary_src: str) -> str:
    base_alias = (primary_src.split()[-1] if " " in primary_src
                  else (primary_src.split("_")[0] if "_" in primary_src else "mas"))
    normalized_joins, seen_joins = [], set()

    # ---- Existing logic preserved ----
    extra_joins = []
    for txt in df.get("transformation_rule", pd.Series()).tolist():
        if not isinstance(txt, str):
            continue
        join_candidates = re.findall(
            r"(?i)(LEFT\s+JOIN\s+[A-Za-z0-9_\.]+\s+ON\s+[A-Za-z0-9_\.=\s\(\)']+)", txt
        )
        for jc in join_candidates:
            if "FROM " not in jc.upper() and jc.strip().upper().startswith("LEFT JOIN"):
                cleaned = normalize_join(jc)
                if cleaned and cleaned.lower().strip() not in seen_joins:
                    extra_joins.append(cleaned)
                    seen_joins.add(cleaned.lower().strip())

    for j in extra_joins:
        if j and j.lower().strip() not in seen_joins:
            normalized_joins.append(j)
            seen_joins.add(j.lower().strip())

    for txt in df.get("join_clause", pd.Series()).tolist():
        j = normalize_join(txt)
        if not j:
            continue
        j = re.sub(r"(?<![A-Za-z0-9_])mas\.", f"{base_alias}.", j)
        key = j.lower().strip()
        if key and key not in seen_joins:
            normalized_joins.append(j)
            seen_joins.add(key)

    # Dedupe + scrub existing joins
    pruned, seen_join_keys = [], set()
    for j in normalized_joins:
        if not j or str(j).strip().lower() == "nan":
            continue
        j2 = re.sub(
            r"\s+FROM\s+[A-Za-z0-9_\. ]+(?=(\s+(LEFT|INNER|RIGHT|FULL)\s+JOIN\b|\s*$))", "", j, flags=re.I
        )
        j2 = re.sub(r"\s*;\s*$", "", j2).strip()
        key = re.sub(r"\s+", " ", j2).strip().lower()
        if key and key not in seen_join_keys:
            pruned.append(j2)
            seen_join_keys.add(key)

    normalized_joins = pruned

    # Detect known aliases
    known_aliases = sorted({
        str(x).split()[-1]
        for x in df.get("join_clause", [])
        if isinstance(x, str) and len(str(x).split()) > 1
    })

    # Replace leaks and unify alias style
    unique, seen = [], set()
    for j in normalized_joins:
        if not j:
            continue
        j = re.sub(r"\s+", " ", j).strip()
        for alias in known_aliases:
            j = re.sub(rf"(?<![\w]){re.escape(alias)}\.", f"{base_alias}.", j, flags=re.I)
        j = re.sub(r"(?<![\w])mas\.", f"{base_alias}.", j, flags=re.I)
        j = re.sub(r"(?<![\w])ossbr_2_1\.", f"{base_alias}.", j, flags=re.I)
        key = j.lower().strip()
        if key not in seen:
            seen.add(key)
            unique.append(j)

    # [PATCH v1.1-B] make aliases unique and remove duplicates properly
    normalized_joins = _ensure_unique_join_aliases(unique, base_alias=base_alias)

    joins = [f"  {j}" for j in normalized_joins]
        # 🧩 Auto-deduplicate alias reuse (ref, ref1, ref2, etc.)
    alias_pattern = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\b(?=\s+ON)", re.I)
    alias_counts = {}
    new_joins = []
    for j in normalized_joins:
        m = alias_pattern.search(j)
        if m:
            alias = m.group(1)
            alias_counts[alias] = alias_counts.get(alias, 0) + 1
            if alias_counts[alias] > 1:
                new_alias = f"{alias}{alias_counts[alias]}"
                j = re.sub(rf"\b{alias}\b", new_alias, j)
        new_joins.append(j)
    normalized_joins = new_joins
    # 🧩 Guard for missing ON clause (auto-fix)
    for idx, j in enumerate(normalized_joins):
        if not re.search(r"\bON\b", j, flags=re.I):
            normalized_joins[idx] = j.strip() + " ON 1=1 -- auto-added ON clause"
    join_clause = "\n".join(joins)

    if DEBUG_JOINS and joins:
        _write_debug("joins_debug.log", "==== Deduped/Normalized JOINS ====\n" + "\n".join(joins))

    # ----- Business rules normalization (unchanged) -----
    br_blocks_raw = [business_rules_to_where(txt) for txt in df.get("business_rule", pd.Series()).tolist()]
    seen_rules, br_blocks = set(), []
    for blk in br_blocks_raw:
        key = blk.lower().strip()
        if key and key not in seen_rules:
            br_blocks.append(blk)
            seen_rules.add(key)

    where_lines = [f"-- Business Rule Block #{i+1}\n  {blk}" for i, blk in enumerate(br_blocks) if blk]
    where_clause = "\nWHERE\n  " + "\n  AND ".join(where_lines) if where_lines else ""

    # [PATCH v1.1-C] safer FROM alias handling (avoid "mas mas")
    ps = primary_src.strip()
    if re.search(r"\s+\w+$", ps):
        from_line = f"  FROM {ps}\n"
    else:
        from_line = f"  FROM {ps} {base_alias}\n"

    return (
        "step1 AS (\n"
        f"  SELECT {base_alias}.*\n"
        f"{from_line}"
        f"{join_clause}\n"
        f"{where_clause}\n)"
    )


# ============================================================
# ---------- Final SELECT builder ----------
# ============================================================

def build_final_select(df: pd.DataFrame) -> Tuple[str, List[Dict[str, str]]]:
    lines, audit_rows = [], []
    grouped = df.groupby(df["tgt_column"].str.lower(), dropna=False)

    for tgt_lower, group in grouped:
        tgt = group["tgt_column"].iloc[0]
        tgt_dtype = (group["tgt_datatype"].iloc[0] if "tgt_datatype" in group else "").strip()
        unique_rules = list({(r or "").strip() for r in group.get("transformation_rule", []) if str(r).strip()})
        unique_sources = list({(s or "").strip() for s in group.get("src_column", []) if str(s).strip()})
        merged_note = ""
        if len(unique_rules) > 1:
            merged_note = f"-- NOTE: merged {len(unique_rules)} variations for target column '{tgt}'"
        elif len(group) > 1:
            merged_note = f"-- NOTE: merged {len(group)} duplicate definitions for target column '{tgt}'"
        raw_trans = unique_rules[0] if unique_rules else ""
        src_col = unique_sources[0] if unique_sources else ""
        expr, trailing_comment = transformation_expression(
            raw_trans, target_col=tgt, src_col=src_col, target_datatype=tgt_dtype
        )
        expr = re.split(r"(?i)\s+\bfrom\b", expr)[0]
        expr = re.split(r"(?i)\s+(left|inner|right|full)\s+join\b", expr)[0]
        expr = expr.strip()

        if tgt_dtype and re.fullmatch(r"[-+]?\d+(\.\d+)?", expr.strip().strip("'")):
            inferred = _infer_datatype_from_value(expr, tgt_dtype)
            expr = _cast_to_datatype(expr, inferred)
        elif tgt_dtype and expr.strip().upper() == "NULL":
            inferred = _infer_datatype_from_value(expr, tgt_dtype)
            expr = _cast_to_datatype(expr, inferred)

        expr = re.sub(r";+$", "", expr).strip()

        # [PATCH v1.1-D] guard suspicious expressions before formatting
        expr = _guard_suspicious(expr)

        # pretty CASE formatting (original)
        if expr.strip().upper().startswith("CASE"):
            expr = re.sub(r"(?i)\b(case)\b", r"\1\n  ", expr)
            expr = re.sub(r"(?i)\b(when)\b", r"\n    \1", expr)
            expr = re.sub(r"(?i)\b(then)\b", r"\n      \1", expr)
            expr = re.sub(r"(?i)\b(else)\b", r"\n    \1", expr)
            expr = re.sub(r"(?i)\bend\b", r"\n  END", expr)
        
        # 🧩 CASE auto-repair: ensure every CASE has an END
        case_count = len(re.findall(r"(?i)\bCASE\b", expr))
        end_count = len(re.findall(r"(?i)\bEND\b", expr))
        if case_count > end_count:
            missing = case_count - end_count
            expr = expr.rstrip() + ("\n  END" * missing)
            _write_debug(
                "sql_validator.log",
                f"Auto-fixed {missing} missing END(s) for CASE in column '{tgt}'"
            )

        expr = re.sub(r"\bLEFT\s+AS\b", "AS", expr, flags=re.I)
        expr = re.sub(r"\bLEFT\s+JOIN\b", "", expr, flags=re.I)

        if not re.search(r"(?i)\bas\s+\w+\b\s*$", expr.strip()):
            select_line = f"    {expr} AS {re.sub(r'[^\w]', '_', tgt)}"
        else:
            select_line = f"    {expr}"

        if merged_note:
            select_line = f"    {merged_note}\n{select_line}"
        if trailing_comment:
            select_line = f"{select_line}\n    {trailing_comment}"

        lines.append(select_line)
        audit_rows.append({
            "row": f"{group.index.min() + 1}",
            "target": tgt,
            "raw": raw_trans.replace("\n", " ").strip(),
            "sql": expr.strip(),
            "note": merged_note or (trailing_comment or "")
        })

        # [PATCH v1.1-E] restore per-column debug log
        try:
            _debug_log("TRANSFORMATION FINAL", f"Target: {tgt}\nType: {tgt_dtype}\nSQL: {expr}")
        except Exception:
            pass

    return "SELECT\n" + ",\n".join(lines) + "\nFROM step1", audit_rows

def infer_sources(df: pd.DataFrame) -> list:
    """Identify unique source tables from mapping."""
    return list(df["src_table"].dropna().unique())

def infer_target(df: pd.DataFrame) -> str:
    """Identify target table from mapping."""
    if "tgt_table" in df.columns:
        tgt_tables = [t for t in df["tgt_table"].unique() if str(t).strip()]
        return tgt_tables[0] if tgt_tables else "UNKNOWN"
    return "UNKNOWN"

def choose_primary(df: pd.DataFrame) -> str:
    """
    Pick the representative source for the main FROM clause.

    Strategy:
    1) If 'src_table' exists and has values, choose the most frequent table.
       - Ignore any alias that may already be present in that cell.
       - Return "<table> mas" so downstream rules that reference 'mas.' keep working.
    2) Else, attempt to infer a table name from join clauses.
    3) Else, fall back to a neutral placeholder with alias 'mas'.
    """
    # 1) Use the most common src_table, if present
    if "src_table" in df.columns:
        # Normalize to just the table token (strip any inline alias)
        tables = []
        for raw in df["src_table"].dropna().astype(str):
            raw = raw.strip()
            if not raw:
                continue
            # allow forms like "OSSBR_2_1 mas" or "OSSBR_2_1"
            parts = raw.split()
            table = parts[0]
            tables.append(table)
        if tables:
            from collections import Counter
            primary_table = Counter(tables).most_common(1)[0][0]
            return f"{primary_table} mas"

    # 2) Try to infer from join_clause if available
    if "join_clause" in df.columns:
        import re
        for raw in df["join_clause"].dropna().astype(str):
            s = raw.strip()
            if not s:
                continue
            # Look for "<JOIN> <table> <alias> ON" or "<JOIN> <table> ON"
            m = re.search(r"(?i)\bjoin\s+([A-Za-z0-9_]+)(?:\s+[A-Za-z0-9_]+)?\s+on\b", s)
            if m:
                inferred_table = m.group(1)
                if inferred_table:
                    return f"{inferred_table} mas"

    # 3) Last-resort placeholder (no hard-coded table name)
    return "source_table mas"

def build_cte_sources(sources: list) -> tuple[list[str], list[str]]:
    """
    Build a list of CTE definitions from given source tables.
    Ensures unique aliases (src, src1, src2, etc.) and
    returns both the CTE SQL snippets and the alias list.
    """
    import re
    ctes, seen_aliases, aliases_out = [], set(), []

    for s in sources:
        parts = str(s).split()
        if len(parts) >= 2:
            table, alias = parts[0], parts[-1]
        else:
            table = s.strip()
            alias = table.split("_")[0] if "_" in table else "src"

        # sanitize alias
        base = re.sub(r"[^A-Za-z0-9_]", "", alias) or "src"
        alias_u = base
        i = 1
        while alias_u.lower() in seen_aliases:
            alias_u = f"{base}{i}"
            i += 1

        seen_aliases.add(alias_u.lower())
        aliases_out.append(alias_u)
        ctes.append(f"{alias_u} AS (SELECT * FROM {table} {alias_u})")

    return ctes, aliases_out

# ---------- Pipeline builder ----------

def build_sql_cte_pipeline(df: pd.DataFrame, target_table: str) -> Tuple[str, List[Dict[str, str]]]:
    sources = infer_sources(df)
    cte_sources, _aliases = build_cte_sources(sources)
    primary = choose_primary(df)
    step1 = build_step1_cte(df, primary_src=primary)
    final_select, audit_rows = build_final_select(df)

    sql_text = "WITH\n" + ",\n".join(cte_sources + [step1]) + "\n" + final_select + ";\n"
    # 🧹 Auto-fix minor SQL issues
    sql_text = re.sub(r"\bFLAOT\b", "FLOAT", sql_text, flags=re.I)
    sql_text = re.sub(r"\bAS\s+[A-Za-z0-9_\.]+\s+AS\s+", "AS ", sql_text, flags=re.I)
    sql_text = re.sub(r"\s+LEFT\s+JOIN", "\n  LEFT JOIN", sql_text)
    sql_text = re.sub(r",\s*LEFT\s+JOIN", ",\n  LEFT JOIN", sql_text)
    # Remove mid-CASE comments that break ELSE/END
    sql_text = re.sub(r"(--[^\n]*)\n\s*ELSE", r"\nELSE", sql_text)
    sql_text = re.sub(r"(--[^\n]*)\n\s*END", r"\nEND", sql_text)
    sql_text = sql_text.replace("  ", " ")

        # Fix dangling 'LEFT AS' tokens and duplicate ref joins
    sql_text = re.sub(r"\s+LEFT\s+AS\s+", " AS ", sql_text, flags=re.I)

    # Remove duplicate identical LEFT JOIN lines (same table & condition)
    deduped_lines = []
    seen_joins = set()
    for line in sql_text.splitlines():
        if line.strip().upper().startswith("LEFT JOIN"):
            norm = re.sub(r"\s+", " ", line.strip().lower())
            if norm not in seen_joins:
                seen_joins.add(norm)
                deduped_lines.append(line)
        else:
            deduped_lines.append(line)
    sql_text = "\n".join(deduped_lines)


    return sql_text, audit_rows


# ---------- JSON builder ----------

def build_job_json(source_malcode: str, target_table: str, sql_path: str, df: pd.DataFrame) -> Dict[str, Any]:
    sources = infer_sources(df)
    sourcelist = []
    for s in sources or []:
        s = str(s).strip()
        if not s:
            continue
        parts = s.split()
        table = parts[0] if parts else s
        sourcelist.append(table)

    source_list_str = ", ".join(sourcelist)

    modules = {}
    dsp = {
        "options": {"module": "data_sourcing_process", "method": "process"},
        "loggable": True,
        "sourcelist": sourcelist
    }
    for s in sources or []:
        s = str(s).strip()
        if not s:
            continue
        parts = s.split()
        table = parts[0] if parts else s
        if not table:
            continue
        dsp[table] = {
            "type": "sz_zone",
            "table.name": table,
            "read-format": "view",
            "path": "${adls.source.root}/" + table
        }
    modules["data_sourcing_process"] = dsp

    modules[f"dt_{target_table.lower()}_{source_malcode.lower()}"] = {
        "sql": f"@{sql_path}",
        "loggable": True,
        "options": {"module": "data_transformation", "method": "process"},
        "name": f"dt_{target_table.lower()}_{source_malcode.lower()}"
    }

    modules["load_enrich_process"] = {
        "options": {"module": "load_enrich_process", "method": "process"},
        "loggable": True,
        "sql": f"SELECT * FROM dt_{target_table.lower()}_{source_malcode.lower()}",
        "target-path": f"${{adls.stage.root}}/{source_malcode}",
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
        "comment": f"This job is responsible for loading data into {target_table} from {source_malcode} - {source_list_str}",
        "modules": modules
    }

# ---------- Audit markdown ----------

def write_audit_md(audit_rows: List[Dict[str, str]], md_path: str):
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# Transformation Rules Audit\n\n")
        f.write("| Row | Target Column | Raw Transformation (verbatim) | Parsed SQL Expression | Notes |\n")
        f.write("|---:|---|---|---|---|\n")
        for r in audit_rows:
            raw = (r["raw"] or "").replace("\n", "<br>").replace("|", "\\|")
            sql = (r["sql"] or "").replace("\n", "<br>").replace("|", "\\|")
            note = (r["note"] or "").replace("\n", "<br>").replace("|", "\\|")
            f.write(f"| {r['row']} | `{r['target']}` | {raw} | `{sql}` | {note} |\n")

# ---------- Orchestration ----------
# ---------- SQL VALIDATION & SANITY CHECKS ----------

def validate_sql(sql_text: str) -> list[str]:
    """
    Lightweight validator to detect obvious SQL issues before writing output.
    Returns a list of error strings.
    """
    import re
    from collections import Counter
    errors = []

    # 1️⃣  Detect duplicate alias usage
    aliases = re.findall(r"\bJOIN\s+[A-Za-z0-9_]+\s+([A-Za-z0-9_]+)\b", sql_text, flags=re.I)
    if aliases:
        dupes = [a for a, c in Counter(aliases).items() if c > 1]
        if dupes:
            errors.append(f"Duplicate aliases found: {', '.join(dupes)}")

    # 2️⃣  Detect JOINs missing ON clause
    join_lines = re.findall(r"LEFT\s+JOIN\s+[A-Za-z0-9_]+\s+[A-Za-z0-9_]+(?![^\\n]*\\bON\\b)", sql_text, flags=re.I)
    if join_lines:
        errors.append(f"JOINs missing ON clause detected: {len(join_lines)} potential issues")

    # 3️⃣  Check for unbalanced CASE/END
    if sql_text.upper().count("CASE") != sql_text.upper().count("END"):
        errors.append("Unbalanced CASE/END blocks")

    # 4️⃣  Mismatched parentheses
    if sql_text.count("(") != sql_text.count(")"):
        errors.append("Mismatched parentheses count")

    # 5️⃣  Undefined aliases (used before declared)
    used_aliases = set(re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\.", sql_text))
    declared_aliases = set(re.findall(r"\bAS\s*\(\s*SELECT\s+\*\s+FROM\s+[A-Za-z0-9_]+\s+([A-Za-z0-9_]+)\)", sql_text))
    missing = used_aliases - declared_aliases - {"mas", "ref"}
    if missing:
        errors.append(f"Potential undefined aliases: {', '.join(sorted(missing))}")

    return errors


def generate(csv_path: str, outdir: str, source_malcode: str = "ND") -> Dict[str, str]:
    df = load_mapping(csv_path)
    target = infer_target(df)
    os.makedirs(outdir, exist_ok=True)
    job_dir = os.path.join(outdir, f"{target.lower()}_job")
    os.makedirs(job_dir, exist_ok=True)

    sql_text, audit_rows = build_sql_cte_pipeline(df, target)
    # 🧩 SQL VALIDATION PASS
    validation_issues = validate_sql(sql_text)

    if validation_issues:
        _write_debug("sql_validator.log", "==== SQL VALIDATION ISSUES ====\n" + "\n".join(validation_issues))
        print(f"[!] {len(validation_issues)} SQL validation issues logged to debug_outputs/sql_validator.log")
    else:
        print("[✓] SQL syntax structure passed validation checks")

    sql_path = os.path.join(job_dir, f"{target.lower()}_{source_malcode.lower()}.sql")
    with open(sql_path, "w", encoding="utf-8") as f:
        f.write(sql_text)

    job_json = build_job_json(source_malcode, target, sql_path, df)
    json_path = os.path.join(job_dir, f"ew_123_{target.lower()}_{source_malcode.lower()}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(job_json, f, indent=2)

    md_path = os.path.join(job_dir, f"transformation_{target.lower()}_{source_malcode.lower()}_rules_audit.md")
    write_audit_md(audit_rows, md_path)

    print(f"[✓] Generated SQL: {sql_path}")
    print(f"[✓] Generated JSON: {json_path}")
    print(f"[✓] Generated Audit Markdown: {md_path}")

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