#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
lookup_reference_module_v3.py
-------------------------------------------------------
Fixes:
 - Associates each source malcode with only its own _cd target columns.
 - Uses Source Schema Name (if applicable) column for mapping.
-------------------------------------------------------
"""

import argparse, json, math, re
import pandas as pd
from pathlib import Path


def s(x) -> str:
    """Clean string safely."""
    if x is None:
        return ""
    try:
        if isinstance(x, float) and math.isnan(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def extract_malcode(val: str) -> str:
    """Extract MCB from MCB{RZ,FS}/..."""
    v = s(val)
    if not v or v.lower() in {"0", "nan", "none"}:
        return ""
    return v.split("{", 1)[0].split("/")[0].strip().upper()


def build_lookup_block(malcode, cd_cols):
    """Build lookup reference SQL block for one malcode."""
    sql = []
    sql.append(f"lk_{malcode.lower()}_cd: {{")
    sql.append("    sql: \"\"\"")
    sql.append("    select source_value1, stndrd_cd_name, stndrd_cd_value")
    sql.append("    from delta.`\"\"\"${adls.lookup.path}\"\"\"/caedw_standard_code_mapping_rules`")
    sql.append("    where (")
    sql.append(f"        source_cd in ('{malcode}')")
    if cd_cols:
        sql.append(f"        and stndrd_cd_name in ({', '.join([f'\'{c}\'' for c in sorted(cd_cols)])})")
    sql.append("    )\"\"\",")
    sql.append("    loggable: true,")
    sql.append("    options: {")
    sql.append("        module: data_transformation,")
    sql.append("        method: process")
    sql.append("    },")
    sql.append(f"    name: lk_{malcode.lower()}_cd")
    sql.append("}")
    return "\n".join(sql)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to fixed (3) CSV file")
    ap.add_argument("--v7c", required=True, help="Path to source_columns_summary_v7c.json")
    ap.add_argument("--out", required=True, help="Output file path")
    args = ap.parse_args()

    df = pd.read_csv(args.csv, dtype=str, keep_default_na=False).fillna("")
    v7c = json.loads(Path(args.v7c).read_text())

    # --- Build malcode → target_columns map
    malcode_to_cdcols = {}
    for _, row in df.iterrows():
        src_schema = extract_malcode(row.get("Source Schema Name (if applicable) (auto populate)", ""))
        tgt_col = s(row.get("Target Column/Field Name * (auto populate)", "")).lower()
        if not src_schema or not tgt_col.endswith("_cd"):
            continue
        malcode_to_cdcols.setdefault(src_schema, set()).add(tgt_col)

    if not malcode_to_cdcols:
        print("⚠️ No _cd mappings found.")
        return

    # --- Generate one block per malcode
    blocks = []
    for malcode, cols in malcode_to_cdcols.items():
        blocks.append(build_lookup_block(malcode, cols))

    Path(args.out).write_text("\n\n".join(blocks), encoding="utf-8")

    print(f"✅ Generated {len(blocks)} lookup reference block(s): {list(malcode_to_cdcols.keys())}")
    print(f"📄 Output written to: {args.out}")


if __name__ == "__main__":
    main()
