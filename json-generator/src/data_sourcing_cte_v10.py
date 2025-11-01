#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
data_sourcing_cte_v10.py

Purpose
-------
Build the data sourcing module in your semi-JSON style:

- Derive malcodes ONLY from:
  "Source Schema Name (if applicable) (auto populate)"
  by taking the prefix before '{' (e.g., "MCB{RZ,FS}/..." -> "MCB").

- Ignore rows where that source schema name is blank/0/nan.

- Group source tables by malcode. For each malcode, list only the
  tables whose *source schema name* points to that malcode.

- Emit a single block that contains:
    source.malcode, source.basepath,
    source.malcode1, source.basepath1, ...
  followed by modules.data_sourcing_process, with a combined sourceList
  and per-table entries. Each table’s path uses the malcode-indexed root:
      first malcode → ${adls.source.root}
      second        → ${adls.source.root1}
      third         → ${adls.source.root2}
      ... and so on.

- Keys are unquoted; string values are quoted.
  Paths are like: "${adls.source.root}/table"

CSV Header Handling
-------------------
We look for these exact headers (as you updated them):
  "Source Schema Name (if applicable) (auto populate)"
  "Source Table/File Name * (auto populate)"
  "Target Table/File Name * (auto populate)"

If headers vary in whitespace/case slightly, we normalize by lowercase match.
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
        if isinstance(x, float) and (math.isnan(x)):
            return ""
    except Exception:
        pass
    return str(x).strip()


def header_lookup(columns, needle_lower):
    """
    Find a column whose lowercase matches the provided lowercase needle.
    Returns the original column name or None.
    """
    lowmap = {c.lower(): c for c in columns}
    return lowmap.get(needle_lower)


def extract_malcode(src_schema_val: str) -> str:
    """
    From "MCB{RZ,FS}/..." → "MCB"
    From "AAW{CZ,DW}/..." → "AAW"
    If value is empty/0/nan → ""
    """
    v = clean_str(src_schema_val)
    if not v:
        return ""
    if v.lower() in {"0", "nan", "none"}:
        return ""
    # Take the part before '{' if present; else the first token [A-Za-z0-9_]+
    before_brace = v.split("{", 1)[0]
    m = re.match(r"^\s*([A-Za-z0-9_]+)", before_brace)
    if not m:
        return ""
    return m.group(1).upper()


def build_output_block(df: pd.DataFrame) -> str:
    # Resolve key columns (case-insensitive)
    cols = list(df.columns)
    src_schema_col = header_lookup(cols, "source schema name (if applicable) (auto populate)")
    src_table_col  = header_lookup(cols, "source table/file name * (auto populate)")
    tgt_table_col  = header_lookup(cols, "target table/file name * (auto populate)")

    if not src_schema_col or not src_table_col or not tgt_table_col:
        missing = []
        if not src_schema_col: missing.append("Source Schema Name (if applicable) (auto populate)")
        if not src_table_col:  missing.append("Source Table/File Name * (auto populate)")
        if not tgt_table_col:  missing.append("Target Table/File Name * (auto populate)")
        raise ValueError(f"Missing expected header(s): {', '.join(missing)}")

    # Determine target table name from first row (for the comment line)
    target_table = clean_str(df.iloc[0][tgt_table_col]) or "target_table"

    # Collect malcodes from Source Schema Name (ignore blank/0/nan)
    # and group source tables by malcode.
    malcode_to_tables = {}
    for _, row in df.iterrows():
        mal = extract_malcode(row.get(src_schema_col, ""))
        if not mal:
            continue
        src_table = clean_str(row.get(src_table_col, "")).lower()
        if not src_table:
            continue
        malcode_to_tables.setdefault(mal, set()).add(src_table)

    # If nothing valid, return a minimal block
    if not malcode_to_tables:
        return (
            "{\n"
            f"    source.malcode: \"unknown\",\n"
            f"    source.basepath: \"UNKNOWN\",\n"
            f"    comment: \"This job is responsible for loading the data into {target_table} from unknown sources: \"\n\n"
            "    modules: {\n"
            "        data_sourcing_process:\n"
            "        {\n"
            "            options:\n"
            "            {\n"
            "                module: data_sourcing_process,\n"
            "                method: process\n"
            "            },\n"
            "            loggable: true,\n"
            "            sourceList: []\n"
            "        }\n"
            "    }\n"
            "}\n"
        )

    # Stable order for malcodes and tables
    malcodes_sorted = sorted(malcode_to_tables.keys())
    # Combined source list in the order of malcodes, then table name
    combined_sources = []
    seen_tables = set()
    for mal in malcodes_sorted:
        for t in sorted(malcode_to_tables[mal]):
            if t not in seen_tables:
                seen_tables.add(t)
                combined_sources.append(t)

    # Build the header lines (source.malcode, source.malcode1, ...)
    lines = []
    lines.append("{")
    for i, mal in enumerate(malcodes_sorted):
        key_suffix = "" if i == 0 else str(i)  # "", "1", "2", ...
        lines.append(f"    source.malcode{key_suffix}: \"{mal.lower()}\",")
        lines.append(f"    source.basepath{key_suffix}: \"{mal.upper()}\",")
        if i == 0:
            # first malcode comment
            tables = ", ".join(sorted(malcode_to_tables[mal]))
            lines.append(f"    comment: \"This job is responsible for loading the data into {target_table} from {mal.lower()} sources: {tables}\"")
        else:
            tables = ", ".join(sorted(malcode_to_tables[mal]))
            lines.append(f"    comment{key_suffix}: \"This job is responsible for loading the data into {target_table} from {mal.lower()} sources: {tables}\"")
        lines.append("")

    # modules/data_sourcing_process
    lines.append("    modules: {")
    lines.append("        data_sourcing_process:")
    lines.append("        {")
    lines.append("            options:")
    lines.append("            {")
    lines.append("                module: data_sourcing_process,")
    lines.append("                method: process")
    lines.append("            },")
    lines.append("            loggable: true,")
    # sourceList array with quoted names
    sl = ", ".join([f"\"{t}\"" for t in combined_sources])
    lines.append(f"            sourceList: [{sl}]")

    # Table blocks. Choose root index based on the malcode that table belongs to.
    # First malcode → root, second → root1, third → root2, etc.
    # If a table appears under multiple malcodes (unlikely), we use the first in sorted order.
    table_to_root_index = {}
    for idx, mal in enumerate(malcodes_sorted):
        for t in sorted(malcode_to_tables[mal]):
            table_to_root_index.setdefault(t, idx)  # first assignment wins

    for t in combined_sources:
        root_suffix = "" if table_to_root_index[t] == 0 else str(table_to_root_index[t])
        root_var = "${adls.source.root}" if root_suffix == "" else f"${{adls.source.root{root_suffix}}}"
        lines.append("")
        lines.append(f"            {t}:")
        lines.append("            {")
        lines.append("                type: sz_zone,")
        lines.append(f"                table_name: {t},")
        lines.append("                read-format: view,")
        lines.append(f"                path: \"{root_var}/" + f"{t}\"")
        lines.append("            }")

    lines.append("        }")
    lines.append("    }")
    lines.append("}")
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", help="Path to the updated fixed (3) CSV file")
    ap.add_argument("--out", required=True, help="Output file path (e.g., data_sourcing_module_v10.txt)")
    args = ap.parse_args()

    df = pd.read_csv(args.csv, dtype=str, keep_default_na=False).fillna("")
    # Normalize column names: keep original but make lookups case-insensitive in builder
    # (No changes to df columns in place; builder resolves case-insensitive)
    out_text = build_output_block(df)
    Path(args.out).write_text(out_text, encoding="utf-8")
    print(f"✅ Wrote data sourcing module to: {args.out}")


if __name__ == "__main__":
    main()
