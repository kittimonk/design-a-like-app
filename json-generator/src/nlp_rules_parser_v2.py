#!/usr/bin/env python3
"""
nlp_rules_parser_v2.py
---------------------------------
Enhanced NLP parser for source-target mapping CSVs.

Goals:
  - Cleanly extract aliases, WHERE filters, and CASE expressions.
  - Remove duplicate JOIN lines or irrelevant text from WHERE.
  - Distinguish CASE expressions vs filter-like conditions.
"""

import argparse, json, re
import pandas as pd
from pathlib import Path
from typing import Dict, List, Set, Tuple
from common_utils import (
    load_csv,
    s,
    learn_source_columns,
    find_alias_for_source,
    harvest_identifiers_for_source,
    strip_from_join,
)


# ---------------------------------------------------------------------
# 🧠 Helper: classify lines from developer free text
# ---------------------------------------------------------------------
def extract_case_and_filter_blocks(texts: List[str]) -> Tuple[List[str], List[str]]:
    """Return (case_blocks, where_blocks) from developer free-text."""
    case_blocks, where_blocks = [], []

    for raw in texts:
        t = s(raw)
        if not t:
            continue

        # Normalize line breaks and spacing
        t_norm = re.sub(r"\s+", " ", t)

        # Identify CASE blocks
        if re.search(r"\bCASE\b", t_norm, re.I):
            # Cut off trailing FROM or JOIN content
            clean_case = strip_from_join(t_norm)
            # Keep only first CASE..END block if multiple present
            if "END" in clean_case.upper():
                segs = re.findall(r"(?is)(CASE .*? END)", clean_case)
                for seg in segs:
                    case_blocks.append(seg.strip())
            else:
                case_blocks.append(clean_case.strip())
            continue

        # Otherwise, treat as potential filter
        if re.search(r"\bWHERE\b", t_norm, re.I):
            cond = re.split(r"(?i)\bwhere\b", t_norm)[-1]
            where_blocks.append(cond.strip())
        elif any(op in t_norm.upper() for op in ["=", "<>", ">", "<", " IN ", " LIKE "]):
            where_blocks.append(t_norm.strip())

    # Deduplicate preserving order
    seen_case, seen_where = set(), set()
    case_blocks = [c for c in case_blocks if not (c.lower() in seen_case or seen_case.add(c.lower()))]
    where_blocks = [w for w in where_blocks if not (w.lower() in seen_where or seen_where.add(w.lower()))]

    return case_blocks, where_blocks


# ---------------------------------------------------------------------
# 🧩 Main NLP parser
# ---------------------------------------------------------------------
def parse_rules(csv_path: str, outdir: str) -> None:
    df = load_csv(csv_path)

    # Collect text per source
    per_source: Dict[str, Dict[str, List[str]]] = {}
    for src in sorted(set(df["src_table"].astype(str).str.strip().str.lower())):
        if not src:
            continue
        sdf = df[df["src_table"].astype(str).str.strip().str.lower() == src]
        texts = []
        for c in ["join_clause", "business_rule", "transformation_rule"]:
            if c in sdf.columns:
                coldata = sdf[c]
                if isinstance(coldata, pd.DataFrame):
                    coldata = coldata.iloc[:, 0]
                texts.extend([s(v) for v in coldata if s(v)])
        per_source[src] = {"texts": texts}

    # Learn known columns from CSV
    src_cols_map = learn_source_columns(df)
    interpretation: Dict[str, Dict] = {}

    # Build interpretation per source
    for src, bundle in per_source.items():
        texts = bundle["texts"]
        alias = find_alias_for_source(src, texts) or src[:4].lower()
        known_cols = src_cols_map.get(src.lower(), set())
        referenced_cols = harvest_identifiers_for_source(src, texts, known_cols, alias)

        # Extract CASE expressions and WHERE-like blocks
        case_blocks, where_blocks = extract_case_and_filter_blocks(texts)

        # Filter out join noise from WHERE (any "JOIN" or table names)
        clean_where = []
        for w in where_blocks:
            if re.search(r"\bJOIN\b", w, re.I):
                continue
            if re.search(r"\bFROM\b", w, re.I):
                continue
            clean_where.append(w)

        interpretation[src] = {
            "alias": alias,
            "known_columns": sorted(list(known_cols)),
            "referenced_columns": referenced_cols,
            "candidate_where_predicates": clean_where,
            "case_like_expressions": case_blocks,
        }

    # Write outputs
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "nlp_rules_interpretation_v2.json"
    md_path = out / "nlp_rules_interpretation_v2.md"

    json_path.write_text(json.dumps(interpretation, indent=2))

    # Markdown report
    lines = ["# NLP Parsing Report v2\n"]
    for src, data in interpretation.items():
        lines.append(f"## Source: {src}")
        lines.append(f"- Alias: `{data['alias']}`")
        lines.append(f"- Known columns: {', '.join(data['known_columns']) or '(none)' }")
        lines.append(f"- Referenced columns: {', '.join(data['referenced_columns']) or '(none)' }")

        if data["candidate_where_predicates"]:
            lines.append("\n### WHERE-like predicates")
            for w in data["candidate_where_predicates"]:
                lines.append(f"- `{w}`")

        if data["case_like_expressions"]:
            lines.append("\n### CASE expressions")
            for e in data["case_like_expressions"]:
                lines.append("```sql")
                lines.append(e)
                lines.append("```")

        lines.append("")

    md_path.write_text("\n".join(lines))

    print(f"✅ NLP interpretation written to:\n  {json_path}\n  {md_path}")


# ---------------------------------------------------------------------
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Enhanced NLP parser for transformation mapping CSVs.")
    p.add_argument("csv", help="Path to source-target mapping CSV")
    p.add_argument("--outdir", required=True, help="Output directory for results")
    args = p.parse_args()
    parse_rules(args.csv, args.outdir)
