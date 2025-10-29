#!/usr/bin/env python3
"""
nlp_rules_parser_v3.py
---------------------------------
Enhanced NLP parser for source-target mapping CSVs.

Upgrades from v2:
  ✅ Fix alias detection (no more alias="ON")
  ✅ Auto-generate WHERE filters (e.g., SRSTATUS='A')
  ✅ Smarter CASE identifier harvesting
  ✅ Ignore placeholder text like "need to know" or "check entity"
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
# 🧠 Enhanced CASE and WHERE extraction
# ---------------------------------------------------------------------
def extract_case_and_filter_blocks(texts: List[str]) -> Tuple[List[str], List[str]]:
    """Return (case_blocks, where_blocks) from developer free-text."""
    case_blocks, where_blocks = [], []
    for raw in texts:
        t = s(raw)
        if not t:
            continue

        t_norm = re.sub(r"\s+", " ", t)

        # Detect CASE
        if re.search(r"\bCASE\b", t_norm, re.I):
            clean_case = strip_from_join(t_norm)
            segs = re.findall(r"(?is)(CASE .*? END)", clean_case)
            if segs:
                for seg in segs:
                    case_blocks.append(seg.strip())
            else:
                case_blocks.append(clean_case.strip())
            continue

        # Detect WHERE/conditions
        if re.search(r"\bWHERE\b", t_norm, re.I):
            cond = re.split(r"(?i)\bwhere\b", t_norm)[-1]
            where_blocks.append(cond.strip())
        elif any(op in t_norm.upper() for op in ["=", "<>", ">", "<", " IN ", " LIKE "]):
            where_blocks.append(t_norm.strip())

    # Dedup + filter placeholders
    ignore_terms = ["need to know", "check", "entity details", "for info"]
    clean_where = []
    seen = set()
    for w in where_blocks:
        wl = w.lower().strip()
        if any(it in wl for it in ignore_terms):
            continue
        if wl not in seen:
            seen.add(wl)
            clean_where.append(w.strip())

    # Dedup CASE
    seen_case = set()
    clean_case = []
    for c in case_blocks:
        cl = c.lower().strip()
        if cl not in seen_case:
            seen_case.add(cl)
            clean_case.append(c.strip())

    return clean_case, clean_where


# ---------------------------------------------------------------------
# 🧩 Alias fixer: smarter JOIN parsing
# ---------------------------------------------------------------------
def find_alias_for_source_fixed(source: str, texts: List[str]) -> str:
    """Improved alias inference that ignores 'ON' and bad captures."""
    src = source.lower()
    for txt in texts:
        # Match FROM ... alias
        for m in re.finditer(r"(?i)\bfrom\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\b(?!\s+on\b)", txt):
            tbl, alias = m.group(1), m.group(2)
            if tbl.split(".")[-1].lower() == src:
                return alias
        # Match JOIN ... alias
        for m in re.finditer(r"(?i)\bjoin\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\b(?!\s+on\b)", txt):
            tbl, alias = m.group(1), m.group(2)
            if tbl.split(".")[-1].lower() == src:
                return alias
    return source[:4].lower()


# ---------------------------------------------------------------------
# 🔍 Smart column enrichment for CASE identifiers
# ---------------------------------------------------------------------
def enrich_columns_from_case(src: str, alias: str, texts: List[str], known_cols: Set[str]) -> Set[str]:
    """Add extra referenced columns found inside CASE blocks."""
    extra = set()
    case_texts = [t for t in texts if "CASE" in t.upper()]
    for txt in case_texts:
        for (qual, col) in re.findall(r"\b([A-Za-z][A-Za-z0-9_]*)\.([A-Za-z][A-Za-z0-9_]*)\b", txt):
            if (qual.lower() in [src.lower(), alias.lower()]) and col not in extra:
                if not re.match(r"^\d+$", col):  # ignore numeric
                    extra.add(col)
    return known_cols.union(extra)


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

    src_cols_map = learn_source_columns(df)
    interpretation: Dict[str, Dict] = {}

    for src, bundle in per_source.items():
        texts = bundle["texts"]
        alias = find_alias_for_source_fixed(src, texts)
        known_cols = src_cols_map.get(src.lower(), set())
        referenced_cols = harvest_identifiers_for_source(src, texts, known_cols, alias)

        # Add more columns found inside CASE
        known_cols = enrich_columns_from_case(src, alias, texts, known_cols)
        referenced_cols = sorted(set(referenced_cols).union(known_cols))

        # Extract CASE + WHERE
        case_blocks, where_blocks = extract_case_and_filter_blocks(texts)

        # Auto-infer SRSTATUS='A' rule
        inferred_filters = []
        for t in texts:
            if re.search(r"SRSTATUS\s*<>\s*'A'", t, re.I) or "exclude inactive" in t.lower():
                inferred_filters.append(f"{alias}.SRSTATUS = 'A'")
        if inferred_filters:
            where_blocks.extend(inferred_filters)

        interpretation[src] = {
            "alias": alias,
            "known_columns": sorted(list(known_cols)),
            "referenced_columns": referenced_cols,
            "candidate_where_predicates": sorted(set(where_blocks)),
            "case_like_expressions": case_blocks,
        }

    # Write outputs
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)
    json_path = out / "nlp_rules_interpretation_v3.json"
    md_path = out / "nlp_rules_interpretation_v3.md"

    json_path.write_text(json.dumps(interpretation, indent=2))

    lines = ["# NLP Parsing Report v3\n"]
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
    p = argparse.ArgumentParser(description="Enhanced NLP parser for transformation mapping CSVs (v3).")
    p.add_argument("csv", help="Path to source-target mapping CSV")
    p.add_argument("--outdir", required=True, help="Output directory for results")
    args = p.parse_args()
    parse_rules(args.csv, args.outdir)
