#!/usr/bin/env python3
"""
data_transformation_cte_v8_1.py
Dynamic SQL View Generator (sanitized and data-driven)

Reads:
  --csv     source_target_mapping_clean_v9_fixed (3).csv
  --summary source_columns_summary_v7c.json
  --lookup  lookup_reference_module_v3.txt
Outputs:
  --out     dt_tantrum_mcb_module_v8_1.txt

Priority for expression source:
  1. JSON derived_columns → 2. JSON static_assignments → 3. CSV rule → 4. NULL
"""

import argparse, csv, json, re, textwrap
from pathlib import Path
from collections import defaultdict, OrderedDict

# --- helper functions ---------------------------------------------------------

def norm_dt(dt: str) -> str:
    if not dt: return "STRING"
    t = dt.strip().lower()
    if t == "decimal": return "DECIMAL(17,2)"
    if t in ("int64","int32","int"): return "BIGINT"
    if t in ("varchar","string"): return "STRING"
    if "date" in t: return "DATE"
    if "timestamp" in t: return "TIMESTAMP"
    return t.upper()

def clean_set_to(text: str) -> str | None:
    """Normalize free-form 'Set to' or 'set A to A' etc into valid SQL expressions."""
    if not text: return None
    t = text.strip()
    if not re.search(r"set\s+to", t, re.I): 
        return None
    t = re.sub(r"(?i)^set\s+to\s+", "", t)
    t = re.sub(r"(?i)set\s+[A-Za-z]+\s+to\s+[A-Za-z]+\s*", "", t)
    t = re.sub(r"[\"'`]", "", t)
    t = re.sub(r"[\.\-–]+$", "", t.strip())
    # cast constants appropriately
    if re.match(r"^\d+(\.\d+)?$", t): 
        return t
    if re.match(r"^\+\d+", t): 
        return f"'{t}'"
    if re.match(r"^current_timestamp", t, re.I): 
        return "current_timestamp()"
    if re.match(r"^null$", t, re.I): 
        return "NULL"
    if "9999-12-31" in t:
        return "to_date('9999-12-31', 'yyyy-MM-dd')"
    if "etl.effective.start.date" in t:
        return "to_date('\"\"\"${etl.effective.start.date}\"\"\"', 'yyyyMMddHHmmss')"
    return f"'{t}'"

def extract_expr_from_rule(rule_text: str, target_col: str) -> str | None:
    if not rule_text: return None
    txt = rule_text.strip()
    # prefer direct CASE / WHEN logic
    if re.search(r"\bCASE\b", txt, re.I): return txt
    m = re.search(r"(?i)\bAS\s+" + re.escape(target_col) + r"\b", txt)
    if m:
        prefix = txt[:m.start()].strip()
        if prefix: return prefix
    if "FROM" in txt.upper():
        prefix = txt.split("FROM",1)[0].strip()
        return prefix
    clean = clean_set_to(txt)
    return clean or None

def cast_if_needed(expr: str, src_dt: str, tgt_dt: str) -> str:
    """Apply casts only when needed; decimals get TRY_CAST."""
    s, t = (src_dt or "").upper(), (tgt_dt or "STRING").upper()
    if not expr or expr.strip().upper() == "NULL": return "NULL"
    if t.startswith("DECIMAL"): return f"TRY_CAST({expr} AS {t})"
    if t in ("BIGINT","INT","DOUBLE","DATE","TIMESTAMP"):
        return f"CAST({expr} AS {t})"
    if t == "STRING" and not expr.strip().startswith("'"): 
        return f"CAST({expr} AS STRING)"
    return expr

# --- main ---------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--summary", required=True)
    ap.add_argument("--lookup", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    summary = json.loads(Path(args.summary).read_text(encoding="utf-8"))
    with open(args.csv, newline="", encoding="utf-8") as f:
        csv_rows = list(csv.DictReader(f))

    alias_for = {e: (meta.get("alias") or e[:4].lower()) for e, meta in summary.items()}
    columns_by_ent = {e: [c.strip().lower() for c in meta.get("referenced_columns", [])]
                      for e, meta in summary.items()}

    derived_by_name = defaultdict(list)
    for e, meta in summary.items():
        for d in meta.get("derived_columns", []) or []:
            nm, ex = d.get("name","").lower(), d.get("expression","")
            if nm and ex: derived_by_name[nm].append(ex)
    static_by_name = defaultdict(list)
    for e, meta in summary.items():
        for s in meta.get("static_assignments", []) or []:
            tgt, val = s.get("target_column","").lower(), s.get("value","")
            if tgt and val: static_by_name[tgt].append(val)

    key_target = "Target Column/Field Name * (auto populate)"
    key_target_dt = "Target Data Type * (auto populate)"
    key_src_tbl = "Source Table/File Name * (auto populate)"
    key_src_col = "Source Column Name * (auto populate)"
    key_src_dt = "Source Data Type * (auto populate)"
    key_rule = "Transformation Rule/Logic (auto populate)"
    key_join = "Join Clause (auto populate)"

    targets = OrderedDict()
    for row in csv_rows:
        tgt = (row.get(key_target) or "").strip()
        if not tgt: continue
        lo = tgt.lower()
        if lo not in targets:
            targets[lo] = {
                "target": tgt,
                "tgt_dt": norm_dt(row.get(key_target_dt,"")),
                "src_tbl": (row.get(key_src_tbl) or "").strip(),
                "src_col": (row.get(key_src_col) or "").strip(),
                "src_dt": norm_dt(row.get(key_src_dt,"")),
                "rule": (row.get(key_rule) or "").strip(),
                "join": (row.get(key_join) or "").strip()
            }

    chosen_expr, chosen_src_dt = {}, {}
    for lo, meta in targets.items():
        tgt = meta["target"]
        expr = None
        # 1. JSON derived
        if not expr and tgt.lower() in derived_by_name:
            expr = derived_by_name[tgt.lower()][0]
        # 2. JSON static
        if not expr and tgt.lower() in static_by_name:
            expr = static_by_name[tgt.lower()][0]
        # 3. CSV rule
        if not expr:
            expr = extract_expr_from_rule(meta["rule"], tgt)
        # 4. fallback
        if not expr and meta["src_tbl"] and meta["src_col"]:
            a = alias_for.get(meta["src_tbl"], meta["src_tbl"][:4].lower())
            expr = f"{a}.{meta['src_col']}"
        if not expr: expr = "NULL"
        chosen_expr[lo] = expr
        chosen_src_dt[lo] = meta["src_dt"]

    # deduplicate
    select_lines, seen = [], set()
    for lo, meta in targets.items():
        if lo in seen: continue
        seen.add(lo)
        tgt = meta["target"]
        expr = chosen_expr[lo]
        casted = cast_if_needed(expr, chosen_src_dt[lo], meta["tgt_dt"])
        select_lines.append(f"    {casted} AS {tgt}")

    # basic join logic and where filters
    base_entity = "ossbr_2_1"
    base_alias = alias_for.get(base_entity,"mas")
    join_lines = [
        "  LEFT JOIN GLSXREF ref ON mas.SRSECCODE = ref.WASTE_SECURITY_CODE",
        "  LEFT JOIN MFSPRIC mfsp ON SUBSTRING(ref.SEND_CD,4,5)=mfsp.PRC_DTL_SEND_NUM",
        "  LEFT JOIN MFIN mfin ON SUBSTRING(ref.SEND_CD,4,5)=mfin.MFIN_SEND_NUMBER"
    ]
    where_lines = [
        "  WHERE qualify row_number() over (partition by mas.srseccode order by mas.srseccode)=1",
        "    AND nullif(regexp_replace(trim(mas.srseccode),'\\s+',''),'') IS NOT NULL",
        "    AND mas.srstatus='A'"
    ]

    with_block = textwrap.dedent(f"""
    WITH step_joined AS (
      SELECT
          mas.srseccode, mas.srstatus, mas.srshsbese, mas.srsectype, mas.srcurrcode,
          mas.secrty_curncy_id, mas.sbdsbdate, mas.srpreq, mas.srpmtrate, mas.srcusipnbr,
          mas.srsecclas, mas.borid,
          ref.send_cd, ref.sm_security_code, ref.fund_desc, ref.waste_security_code,
          ref.fund_company, ref.fund_number, ref.prc_dtl_send_num,
          mfsp.prc_dtl_send_num AS mfsp_prc_dtl_send_num,
          mfin.mfin_send_number, mfin.mfin_asset_order
      FROM {base_entity} {base_alias}
      {"".join(join_lines)}
      {"".join(where_lines)}
    )
    SELECT
    """).strip()

    final_sql = with_block + "\n" + ",\n".join(select_lines) + "\nFROM step_joined s"
    module_text = f"""dt_tantrum_mcb: {{
  sql: \"\"\"\n{final_sql}\n\"\"\",\n  loggable: true,\n  options: {{ module: data_transformation, method: process }},\n  name: "dt_tantrum_mcb"\n}}"""

    Path(args.out).write_text(module_text, encoding="utf-8")
    print(f"[v8.1] SQL module generated → {args.out}")

if __name__ == "__main__":
    main()
