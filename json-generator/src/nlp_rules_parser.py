#!/usr/bin/env python3
import argparse, json, re
from typing import Dict, List, Optional, Set, Tuple
import pandas as pd
from pathlib import Path

# Import helpers (patched common_utils)
from common_utils import (
    load_csv, s,
    learn_source_columns, find_alias_for_source,
    harvest_identifiers_for_source, strip_from_join
)

def parse_rules(csv_path: str, outdir: str) -> None:
    """
    Parse free-text transformation/join/business-rule logic from the mapping CSV
    into a structured NLP interpretation per source table.
    """
    df = load_csv(csv_path)

    # --- Defensive normalization ---
    # In some cases duplicate canonical columns can produce a DataFrame, not Series.
    if isinstance(df.get("src_table"), pd.DataFrame):
        df["src_table"] = df["src_table"].iloc[:, 0]

    # Ensure all required columns exist
    for col in ["join_clause", "business_rule", "transformation_rule"]:
        if col not in df.columns:
            df[col] = ""

    # --- Collect free-text fields per source ---
    per_source = {}
    src_values = sorted(set(df["src_table"].astype(str).str.strip().str.lower()))
    for src in src_values:
        if not src:
            continue
        sdf = df[df["src_table"].astype(str).str.strip().str.lower() == src]
        texts = []
        for c in ["join_clause", "business_rule", "transformation_rule"]:
            ser = sdf[c]
            if isinstance(ser, pd.DataFrame):
                ser = ser.iloc[:, 0]
            texts.extend([s(v) for v in ser if s(v)])
        per_source[src] = {"texts": texts}

    # --- Machine-readable interpretation ---
    interpretation = {}
    src_cols_map = learn_source_columns(df)

    for src, bundle in per_source.items():
        texts = bundle["texts"]
        alias = find_alias_for_source(src, texts) or (src[:4].lower())
        known_cols = src_cols_map.get(src.lower(), set())
        referenced_cols = harvest_identifiers_for_source(src, texts, known_cols, alias)

        # Heuristic WHERE extraction
        where_like = []
        for t in texts:
            for line in re.split(r"\n| and ", t, flags=re.I):
                line = line.strip()
                if not line:
                    continue
                if re.search(rf"\b{re.escape(alias)}\.[A-Za-z][A-Za-z0-9_]*\b", line):
                    where_like.append(line)
                elif re.search(rf"\b{re.escape(src)}\.[A-Za-z][A-Za-z0-9_]*\b", line, flags=re.I):
                    where_like.append(line)

        # Extract CASE-like expressions (cleaned of FROM/JOIN)
        case_exprs = []
        for t in texts:
            if re.search(r"(?i)\bcase\b", t):
                e = strip_from_join(t).strip()
                case_exprs.append(e)

        interpretation[src] = {
            "alias": alias,
            "known_columns": sorted(list(known_cols)),
            "referenced_columns": referenced_cols,
            "candidate_where_predicates": list(dict.fromkeys(where_like)),
            "case_like_expressions": case_exprs,
        }

    # --- Write outputs ---
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "nlp_rules_interpretation.json").write_text(
        json.dumps(interpretation, indent=2)
    )

    # Markdown log
    lines = ["# NLP Parsing Report\n"]
    for src, data in interpretation.items():
        lines.append(f"## Source: {src}")
        lines.append(f"- Alias: `{data['alias']}`")
        lines.append(f"- Known columns: {', '.join(data['known_columns']) or '(none)'}")
        lines.append(
            f"- Referenced columns (from free text): "
            f"{', '.join(data['referenced_columns']) or '(none)'}"
        )
        if data["candidate_where_predicates"]:
            lines.append("- Candidate WHERE predicates:")
            for w in data["candidate_where_predicates"]:
                lines.append(f"  - `{w}`")
        if data["case_like_expressions"]:
            lines.append("- CASE-like expressions (stripped):")
            for e in data["case_like_expressions"]:
                lines.append("```sql")
                lines.append(e)
                lines.append("```")
        lines.append("")
    (out / "nlp_rules_interpretation.md").write_text("\n".join(lines))

    print(f"✅ NLP interpretation written to {out/'nlp_rules_interpretation.json'}")

# --- CLI entrypoint ---
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Parse NLP rules from mapping CSV")
    p.add_argument("csv", help="Path to source-target mapping CSV")
    p.add_argument("--outdir", required=True, help="Directory to write parsed logs")
    args = p.parse_args()
    parse_rules(args.csv, args.outdir)
