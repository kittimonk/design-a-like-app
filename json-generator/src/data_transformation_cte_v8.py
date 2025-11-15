#!/usr/bin/env python3
"""
data_transformation_cte_v8.py
Dynamic SQL View Generator (no hardcoding)

Reads:
  --csv     source_target_mapping_clean_v9_fixed (3).csv
  --summary source_columns_summary_v7c.json
  --lookup  lookup_reference_module_v3.txt
Outputs:
  --out     dt_tantrum_mcb_module_v8.txt

Core logic:
  1. Parse inputs (CSV, JSON summary, lookup text)
  2. Build alias mappings, derived/static maps
  3. Auto-select FROM + JOIN + WHERE + SELECT clauses
  4. Generate Databricks-compatible module text
"""

import argparse, json, csv, re, textwrap
from pathlib import Path
from collections import OrderedDict, defaultdict

def norm_dt(dt: str) -> str:
    """Normalize data type to a consistent upper-case form."""
    if not dt: return "STRING"
    t = dt.strip().lower()
    if t == "decimal": return "DECIMAL(17,2)"
    if t in ("int64", "int32", "int"): return "BIGINT"
    if t in ("varchar", "string"): return "STRING"
    return t.upper()

def extract_expr_from_rule(rule_text: str, target_col: str) -> str | None:
    """Extract SQL expression from a free-form 'Transformation Rule/Logic' text."""
    if not rule_text: return None
    s = rule_text.strip()
    # Look for "AS col"
    m = re.search(rf"(?is)(.+?)\s+AS\s+{re.escape(target_col)}\s*(?:$|\n)", s)
    if m: return m.group(1).strip()
    # Before FROM
    m2 = re.search(r"(?is)^(.+?)\s+FROM\s+.+$", s)
    if m2: return m2.group(1).strip()
    # CASE / Function / SET TO pattern
    if re.search(r"\bcase\b.*\bend\b", s, re.I|re.S): return s
    if "(" in s and ")" in s and not s.lower().startswith("from "): return s
    m3 = re.search(r"set\s+to\s+(.+)$", s, re.I)
    if m3: return m3.group(1).strip().rstrip(".")
    return None

def cast_if_needed(expr: str, src_dt: str, tgt_dt: str) -> str:
    """Apply casts only when needed; decimals get TRY_CAST."""
    s, t = (src_dt or "").upper(), (tgt_dt or "STRING").upper()
    is_plain = bool(re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$", expr.strip()))
    if not is_plain:
        if t.startswith("DECIMAL"): return f"TRY_CAST({expr} AS {t})"
        return expr
    if t == "STRING":
        return f"CAST({expr} AS STRING)" if s and s != "STRING" else expr
    if t.startswith("DECIMAL"):
        return f"TRY_CAST({expr} AS {t})" if s != t else expr
    if t in ("BIGINT","INT","DOUBLE","DATE","TIMESTAMP"):
        return f"CAST({expr} AS {t})" if s and s != t else expr
    return expr

def explicit_cols_for_alias(ent, alias, columns_by_ent):
    cols = columns_by_ent.get(ent, [])
    out, seen = [], set()
    for c in cols:
        if c and c not in seen:
            seen.add(c)
            out.append(f"      {alias}.{c} AS {alias}_{c}")
    if not out:
        out = [f"      {alias}.srseccode AS {alias}_srseccode"]
    return out

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True)
    p.add_argument("--summary", required=True)
    p.add_argument("--lookup", required=True)
    p.add_argument("--out", required=True)
    args = p.parse_args()

    summary = json.loads(Path(args.summary).read_text(encoding="utf-8"))
    with open(args.csv, newline="", encoding="utf-8") as f:
        csv_rows = list(csv.DictReader(f))

    alias_for = { e: (meta.get("alias") or e[:4]) for e, meta in summary.items() }
    columns_by_ent = { e: [c.strip().lower() for c in meta.get("referenced_columns", [])]
                       for e, meta in summary.items() }

    derived_by_name = defaultdict(list)
    for e, meta in summary.items():
        for d in meta.get("derived_columns", []) or []:
            nm, ex = d.get("name","").lower(), d.get("expression","")
            if nm and ex: derived_by_name[nm].append(ex)
    statics_by_name = defaultdict(list)
    for e, meta in summary.items():
        for s in meta.get("static_assignments", []) or []:
            tgt, val = s.get("target_column","").lower(), s.get("value","")
            if tgt and val: statics_by_name[tgt].append(val)

    tgt_key = "Target Column/Field Name * (auto populate)"
    tgt_dt_key = "Target Data Type * (auto populate)"
    src_tbl_key, src_col_key, src_dt_key = (
        "Source Table/File Name * (auto populate)",
        "Source Column Name * (auto populate)",
        "Source Data Type * (auto populate)"
    )
    rule_key, join_key = "Transformation Rule/Logic (auto populate)", "Join Clause (auto populate)"
    targets = OrderedDict()
    for r in csv_rows:
        nm = (r.get(tgt_key) or "").strip()
        if not nm: continue
        lo = nm.lower()
        if lo not in targets:
            targets[lo] = {
                "target": nm,
                "target_dt": norm_dt(r.get(tgt_dt_key, "")),
                "source_table": (r.get(src_tbl_key) or "").strip(),
                "source_col": (r.get(src_col_key) or "").strip(),
                "source_dt": norm_dt(r.get(src_dt_key, "")),
                "rule": (r.get(rule_key) or "").strip(),
                "join": (r.get(join_key) or "").strip(),
            }

    chosen_expr, chosen_src_dt = {}, {}
    for key, meta in targets.items():
        tgt = meta["target"]
        expr = extract_expr_from_rule(meta["rule"], tgt)
        if not expr:
            cand = derived_by_name.get(tgt.lower())
            if cand: expr = cand[0]
        if not expr:
            vals = statics_by_name.get(tgt.lower())
            if vals: expr = vals[0]
        if not expr and meta["source_table"] and meta["source_col"]:
            alias = alias_for.get(meta["source_table"], meta["source_table"][:4].lower())
            expr = f"{alias}.{meta['source_col']}"
        if not expr:
            expr = "NULL"
        chosen_expr[key] = expr
        chosen_src_dt[key] = meta["source_dt"]

    alias_freq = defaultdict(int)
    for expr in chosen_expr.values():
        for a in re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\.", expr):
            alias_freq[a.lower()] += 1
    ent_for_alias = { (meta.get("alias") or e[:4]).lower(): e for e, meta in summary.items() }
    default_from_entity = "ossbr_2_1"
    default_from_alias = alias_for.get(default_from_entity, "mas")
    if alias_freq:
        top_alias = max(alias_freq, key=alias_freq.get)
        base_entity = ent_for_alias.get(top_alias, default_from_entity)
        base_alias = alias_for.get(base_entity, top_alias)
    else:
        base_entity, base_alias = default_from_entity, default_from_alias

    select_items, seen_targets = [], set()
    for key, meta in targets.items():
        col = meta["target"]
        if col.lower() in seen_targets: continue
        seen_targets.add(col.lower())
        expr = chosen_expr[key]
        out = cast_if_needed(expr, chosen_src_dt[key], meta["target_dt"])
        select_items.append(f"    {out} AS {col}")

    join_pairs = []
    for r in csv_rows:
        jtxt = (r.get(join_key) or "").strip()
        if not jtxt: continue
        for ln in [ln.strip() for ln in jtxt.splitlines() if ln.strip()]:
            m = re.search(r"join\s+([A-Za-z0-9_]+)\s+(\w+)\s+with\s+([A-Za-z0-9_]+)\s+(\w+)", ln, re.I)
            if m:
                join_pairs.append((m.group(1), m.group(2), m.group(3), m.group(4)))

    mentioned_aliases = {a.lower() for expr in chosen_expr.values()
                         for a in re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\.", expr)}
    mentioned_aliases.add(base_alias.lower())

    def infer_on_cond(l_alias, r_alias): 
        return f"{l_alias}.srseccode = {r_alias}.srseccode"

    join_clauses, seen_lr = [], set()
    for l_ent, l_alias, r_ent, r_alias in join_pairs:
        if not ({l_alias.lower(), r_alias.lower()} & mentioned_aliases): continue
        key = (l_alias.lower(), r_alias.lower())
        if key in seen_lr: continue
        seen_lr.add(key)
        on = infer_on_cond(l_alias, r_alias)
        join_clauses.append(f"  LEFT JOIN {r_ent} {r_alias} ON {on}")

    where_preds = []
    for ent, meta in summary.items():
        for p in meta.get("candidate_where_predicates", []) or []:
            aliases_in_p = {m.group(1).lower() for m in re.finditer(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\.", p)}
            if not aliases_in_p or aliases_in_p.issubset(mentioned_aliases):
                if p not in where_preds: where_preds.append(p)

    step_select_lines = explicit_cols_for_alias(base_entity, base_alias, columns_by_ent)
    aliases_to_include = set()
    for _, l_alias, r_ent, r_alias in join_pairs:
        if r_alias.lower() in mentioned_aliases:
            aliases_to_include.add((r_ent, r_alias))
    for a in mentioned_aliases:
        ent = ent_for_alias.get(a)
        if ent and a != base_alias.lower():
            aliases_to_include.add((ent, alias_for.get(ent, a)))
    for ent, alias in sorted(aliases_to_include):
        if alias.lower() == base_alias.lower(): continue
        step_select_lines.extend(explicit_cols_for_alias(ent, alias, columns_by_ent))

    with_lines = [
        "WITH step_joined AS (",
        "  SELECT",
        ",\n".join(step_select_lines),
        f"  FROM {base_entity} {base_alias}",
        *join_clauses
    ]
    if where_preds:
        with_lines.append("  WHERE " + " AND ".join(where_preds))
    with_lines.append(")")

    final_sql = "\n".join(with_lines) + "\nSELECT\n" + ",\n".join(select_items) + "\nFROM step_joined s"
    module_text = f"""dt_tantrum_mcb: {{
  sql: \"\"\"\n{final_sql}\n\"\"\",\n  loggable: true,\n  options: {{ module: data_transformation, method: process }},\n  name: "dt_tantrum_mcb"\n}}"""

    Path(args.out).write_text(module_text, encoding="utf-8")
    print(f"[v8] Generated SQL module → {args.out}")

if __name__ == "__main__":
    main()
