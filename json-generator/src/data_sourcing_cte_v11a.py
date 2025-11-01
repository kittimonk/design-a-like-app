#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
data_sourcing_cte_v11a.py

- Extracts source.malcode(s) ONLY from "Source Schema Name (if applicable) (auto populate)"
- Groups source tables by malcode (ignoring 0/blank/nan)
- Keeps the *first-seen order* from the CSV (no alphabetical sort)
- Skips reference tables (source schema = 0)
- Builds semi-JSON structure (unquoted keys, quoted string values)
- Assigns root variables dynamically:
      first malcode  → ${adls.source.root}
      second malcode → ${adls.source.root1}
      third malcode  → ${adls.source.root2}
"""

import argparse
import math
import re
from pathlib import Path
import pandas as pd


def clean_str(x) -> str:
    if x is None:
        return ""
    try:
        if isinstance(x, float) and math.isnan(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def header_lookup(columns, needle_lower):
    lowmap = {c.lower(): c for c in columns}
    return lowmap.get(needle_lower)


def extract_malcode(val: str) -> str:
    """Extract prefix before '{' from Source Schema Name value."""
    v = clean_str(val)
    if not v or v.lower() in {"0", "nan", "none"}:
        return ""
    before_brace = v.split("{", 1)[0]
    m = re.match(r"^\s*([A-Za-z0-9_]+)", before_brace)
    return m.group(1).upper() if m else ""


def build_output_block(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    src_schema_col = header_lookup(cols, "source schema name (if applicable) (auto populate)")
    src_table_col  = header_lookup(cols, "source table/file name * (auto populate)")
    tgt_table_col  = header_lookup(cols, "target table/file name * (auto populate)")

    if not src_schema_col or not src_table_col or not tgt_table_col:
        raise ValueError("❌ Missing one of required columns (Source Schema Name, Source Table/File Name, Target Table/File Name)")

    target_table = clean_str(df.iloc[0][tgt_table_col]) or "target_table"

    # Use OrderedDict-like behavior to preserve CSV appearance order
    malcode_to_tables = {}
    seen_order = []

    for _, row in df.iterrows():
        mal = extract_malcode(row.get(src_schema_col, ""))
        if not mal:
            continue
        src_table = clean_str(row.get(src_table_col, "")).lower()
        if not src_table:
            continue
        if mal not in malcode_to_tables:
            malcode_to_tables[mal] = set()
            seen_order.append(mal)
        malcode_to_tables[mal].add(src_table)

    # ---- Build formatted text ----
    lines = ["{"]
    for i, mal in enumerate(seen_order):
        suffix = "" if i == 0 else str(i)
        lines.append(f"    source.malcode{suffix}: \"{mal.lower()}\",")
        lines.append(f"    source.basepath{suffix}: \"{mal.upper()}\",")
        tables = ", ".join(sorted(malcode_to_tables[mal]))
        comment_key = "comment" if suffix == "" else f"comment{suffix}"
        lines.append(f"    {comment_key}: \"This job is responsible for loading the data into {target_table} from {mal.lower()} sources: {tables}\"")
        lines.append("")

    lines.append("    modules: {")
    lines.append("        data_sourcing_process:")
    lines.append("        {")
    lines.append("            options:")
    lines.append("            {")
    lines.append("                module: data_sourcing_process,")
    lines.append("                method: process")
    lines.append("            },")
    lines.append("            loggable: true,")

    combined_sources, seen_tables = [], set()
    for mal in seen_order:
        for t in sorted(malcode_to_tables[mal]):
            if t not in seen_tables:
                seen_tables.add(t)
                combined_sources.append(t)

    sl = ", ".join([f"\"{t}\"" for t in combined_sources])
    lines.append(f"            sourceList: [{sl}]")

    table_to_root_idx = {}
    for idx, mal in enumerate(seen_order):
        for t in sorted(malcode_to_tables[mal]):
            table_to_root_idx.setdefault(t, idx)

    for t in combined_sources:
        root_suffix = "" if table_to_root_idx[t] == 0 else str(table_to_root_idx[t])
        root_var = "${adls.source.root}" if root_suffix == "" else f"${{adls.source.root{root_suffix}}}"
        lines.extend([
            "",
            f"            {t}:",
            "            {",
            "                type: sz_zone,",
            f"                table_name: {t},",
            "                read-format: view,",
            f"                path: \"{root_var}/" + f"{t}\"",
            "            }"
        ])

    lines.extend(["        }", "    }", "}"])
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", help="Input source-target mapping CSV (fixed v3)")
    ap.add_argument("--out", required=True, help="Output path (e.g., data_sourcing_module_v11a.txt)")
    args = ap.parse_args()

    df = pd.read_csv(args.csv, dtype=str, keep_default_na=False).fillna("")
    out_text = build_output_block(df)
    Path(args.out).write_text(out_text, encoding="utf-8")
    print(f"✅ Wrote data sourcing module to: {args.out}")


if __name__ == "__main__":
    main()
