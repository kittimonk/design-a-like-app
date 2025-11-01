#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data_sourcing_cte_v7.py
--------------------------------------------------------
Tested and validated version that:
- Extracts source.malcode from "Source Schema Name (if applicable) (auto populate)"
- Takes substring before '{'
- Skips blanks, 0, nan
- Builds dynamic blocks per malcode
- Matches exact semi-JSON format requested (no extra quotes)
--------------------------------------------------------
"""

import argparse
import pandas as pd
import re
from pathlib import Path


def s(x) -> str:
    """Safe string clean."""
    if x is None:
        return ""
    try:
        import math
        if isinstance(x, float) and math.isnan(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


def extract_malcode(val: str) -> str:
    """
    Extract the source malcode from the Source Schema Name field.
    E.g., 'MCB{RZ,FS}/mcb@edrz.dfs...' -> 'MCB'
    """
    v = s(val)
    if not v or v.lower() in {"nan", "none", "0"}:
        return ""
    m = re.match(r"^([A-Za-z0-9_]+)", v)
    if m:
        return m.group(1).upper()
    return ""


def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def build_block(malcode: str, df: pd.DataFrame) -> str:
    sources = sorted(
        set([s(r).lower() for r in df["source table/file name * (auto populate)"] if s(r)])
    )
    tgt_table = s(df.iloc[0].get("source table/file name * (auto populate)", "target_table"))
    comment = (
        f"This job is responsible for loading the data into {tgt_table} "
        f"from {malcode.lower()} sources: {', '.join(sources)}"
    )

    lines = []
    lines.append("{")
    lines.append(f"    source.malcode: \"{malcode.lower()}\",")
    lines.append(f"    source.basepath: \"{malcode.upper()}\",")
    lines.append(f"    comment: \"{comment}\"")
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

    # build the quoted source list separately to avoid backslashes inside f-string expressions
    source_list = ", ".join(f'"{src}"' for src in sources)
    lines.append(f"            sourceList: [{source_list}]")

    for src in sources:
        lines.append("")
        lines.append(f"            {src}:")
        lines.append("            {")
        lines.append("                type: sz_zone,")
        lines.append(f"                table_name: {src},")
        lines.append("                read-format: view,")
        # emit a literal ${adls.source.root} by using a normal f-string with braces doubled in format
        lines.append(f'                path: "${{adls.source.root}}/{src}"')
        lines.append("            }")

    lines.append("        }")
    lines.append("    }")
    lines.append("}")
    return "\n".join(lines)


def build_block_for_target(target: str, df: pd.DataFrame, src_schema_col: str) -> str:
    """Build a single combined block for a target table grouping sources by malcode."""
    # Map malcode -> list of source tables (preserve order of appearance)
    malcode_order = []
    malcode_sources = {}

    for _, row in df.iterrows():
        malcode = extract_malcode(row.get(src_schema_col, ""))
        src = s(row.get("source table/file name * (auto populate)", "")).lower()
        if not src:
            continue
        if not malcode:
            # skip rows without a valid source malcode
            continue
        if malcode not in malcode_sources:
            malcode_order.append(malcode)
            malcode_sources[malcode] = []
        if src not in malcode_sources[malcode]:
            malcode_sources[malcode].append(src)

    if not malcode_order:
        # no source malcodes found for this target
        return ""

    # all unique sources across malcodes in appearance order
    all_sources = []
    for m in malcode_order:
        for src in malcode_sources[m]:
            if src not in all_sources:
                all_sources.append(src)

    lines = []
    lines.append("{")

    # emit malcode/basepath/comment entries, numbered suffix for subsequent malcodes
    for idx, m in enumerate(malcode_order):
        suffix = "" if idx == 0 else str(idx)
        comment = (
            f"This job is responsible for loading the data into {target} "
            f"from {m.lower()} sources: {', '.join(malcode_sources[m])}"
        )
        lines.append(f'    source.malcode{suffix}: "{m.lower()}",')
        lines.append(f'    source.basepath{suffix}: "{m.upper()}",')
        lines.append(f'    comment{suffix}: "{comment}"')
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

    # quoted sourceList with all sources
    source_list = ", ".join(f'"{src}"' for src in all_sources)
    lines.append(f"            sourceList: [{source_list}]")

    # helper to get malcode index for a source
    def malcode_index_for_source(src_name: str) -> int:
        for i, mm in enumerate(malcode_order):
            if src_name in malcode_sources.get(mm, []):
                return i
        return 0

    # emit each source table block; path uses adls.source.root, adls.source.root1, etc.
    for src in all_sources:
        idx = malcode_index_for_source(src)
        suffix = "" if idx == 0 else str(idx)
        lines.append("")
        lines.append(f"            {src}:")
        lines.append("            {")
        lines.append("                type: sz_zone,")
        lines.append(f"                table_name: {src},")
        lines.append("                read-format: view,")
        lines.append(f'                path: "${{adls.source.root{suffix}}}/{src}"')
        lines.append("            }")

    lines.append("        }")
    lines.append("    }")
    lines.append("}")

    return "\n".join(lines)


def build_all_modules(df: pd.DataFrame, out_prefix: str):
    # --- Identify correct Source Schema Name column robustly ---
    cols = list(df.columns)
    src_schema_col = None

    # Prefer the exact canonical source schema name column if present
    canonical_src = "source schema name (if applicable) (auto populate)"
    if canonical_src in cols:
        src_schema_col = canonical_src

    # fallback to columns that mention 'schema name' and contain 'source'
    if not src_schema_col:
        candidates = [c for c in cols if "schema name" in c]
        if candidates:
            src_schema_col = next((c for c in candidates if "source" in c), candidates[0])

    if not src_schema_col:
        # fallback: search for any column containing both 'schema' and not looking like target
        for c in cols:
            if "schema" in c and "target" not in c:
                src_schema_col = c
                break

    if not src_schema_col:
        raise ValueError("❌ Could not find the correct Source Schema Name column in CSV.")

    # --- Identify target column explicitly ---
    canonical_tgt = "target table/file name * (auto populate)"
    if canonical_tgt in cols:
        target_col = canonical_tgt
    else:
        target_col = next((c for c in cols if "target table" in c or "target" in c), None)
        if not target_col:
            # fallback to a reasonable default (source table column)
            if "source table/file name * (auto populate)" in cols:
                target_col = "source table/file name * (auto populate)"
            else:
                target_col = cols[0]

    # --- Group rows by target value and build one block per target ---
    outputs = []
    for tgt_value, group in df.groupby(target_col):
        tgt_name = s(tgt_value) or "target_table"
        block = build_block_for_target(tgt_name, group, src_schema_col)
        if block:
            outputs.append(block)

    if not outputs:
        print("⚠️ No valid malcodes found in the Source Schema Name column.")
        return

    out_txt = "\n\n".join(outputs)
    out_file = Path(out_prefix).with_suffix(".txt")
    out_file.write_text(out_txt, encoding="utf-8")

    print(f"✅ Generated {len(outputs)} data sourcing block(s):")
    # list malcodes per block for quick visibility
    for o in outputs:
        # show first malcode line
        first_line = o.splitlines()[1] if len(o.splitlines()) > 1 else ""
        print(f" - {first_line}")
    print(f"📄 Output written to: {out_file}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", help="Input source-target mapping CSV")
    ap.add_argument("--out", required=True, help="Output file prefix (no extension)")
    args = ap.parse_args()

    df = load_csv(args.csv)
    build_all_modules(df, args.out)
