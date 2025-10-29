#!/usr/bin/env python3
"""
nlp_rules_parser_v4.py
---------------------------------
Self-contained NLP parser for source-target mapping CSVs.

Upgrades vs prior:
  - Robust header canonicalization + duplicate-column collapse
  - Alias detection fixed (ignores trailing ON / WITH)
  - JOIN/FROM noise stripped out of filters
  - CASE harvesting enriches referenced columns
  - Auto-infers SRSTATUS='A' from business rules
  - Clean JSON + Markdown outputs
"""

import argparse, json, re
import pandas as pd
from pathlib import Path
from typing import Dict, List, Set, Tuple

# -------------------------- basic utils --------------------------
def s(x) -> str:
    if x is None:
        return ""
    try:
        import math
        if isinstance(x, float) and (pd.isna(x) or math.isnan(x)):
            return ""
    except Exception:
        pass
    return str(x).replace("\r", " ").replace("\n", " ").strip()

CANON_MAP = {
    r"(?i)^source schema id.*$": "src_schema_id",
    r"(?i)^db name/incoming file path.*$": "src_db",
    r"(?i)^schema name.*auto.*$": "src_schema",
    r"(?i)^table/file name.*auto.*$": "src_table",
    r"(?i)^column name.*auto.*$": "src_column",
    r"(?i)^data type.*auto.*$": "src_datatype",
    r"(?i)^target schema id.*$": "tgt_schema_id",
    r"(?i)^db name/outgoing file path.*$": "tgt_db",
    r"(?i)^schema name.*auto.*\.\d+$": "tgt_schema",
    r"(?i)^table/file name.*auto.*\.\d+$": "tgt_table",
    r"(?i)^column/field name.*auto.*$": "tgt_column",
    r"(?i)^data type.*auto.*\.\d+$": "tgt_datatype",
    r"(?i)^business rule.*$": "business_rule",
    r"(?i)^join clause.*$": "join_clause",
    r"(?i)^transformation rule/logic.*$": "transformation_rule",
}

QUAL_ID_RX  = re.compile(r"\b([A-Za-z][A-Za-z0-9_]*)\.([A-Za-z][A-Za-z0-9_]*)\b")

def canon_headers(df: pd.DataFrame) -> pd.DataFrame:
    cols = []
    for c in df.columns:
        mapped = None
        for rx, tgt in CANON_MAP.items():
            if re.match(rx, str(c).strip()):
                mapped = tgt
                break
        cols.append(mapped or str(c))
    new_df = df.copy()
    new_df.columns = cols
    return new_df

def _collapse_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns)
    seen = {}
    for i, c in enumerate(cols):
        seen.setdefault(c, []).append(i)
    out = pd.DataFrame(index=df.index)
    for name, idxs in seen.items():
        if len(idxs) == 1:
            out[name] = df.iloc[:, idxs[0]]
        else:
            merged = df.iloc[:, idxs[0]].astype(str)
            for j in idxs[1:]:
                cur = df.iloc[:, j].astype(str)
                merged = merged.where(merged.str.strip() != "", cur)
            out[name] = merged
    return out

def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")
    df = canon_headers(df)
    df = _collapse_duplicate_columns(df)
    required = ["src_table","src_column","tgt_table","tgt_column","business_rule","join_clause","transformation_rule"]
    for c in required:
        if c not in df.columns:
            df[c] = ""
        df[c] = df[c].astype(str)
    df["src_table"] = df["src_table"].apply(lambda v: str(v).strip().split()[0].lower() if str(v).strip() else "")
    return df

def learn_source_columns(df: pd.DataFrame) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}
    for _, row in df.iterrows():
        t = str(row.get("src_table","")).strip().lower()
        c = str(row.get("src_column","")).strip()
        if not t or not c:
            continue
        if re.match(r"(?i)^t_[a-z0-9_]+_\d+$", c):
            continue
        if c.lower() == "nan":
            continue
        out.setdefault(t, set()).add(c)
    return out

def strip_from_join(expr: str) -> str:
    e = re.split(r"(?i)\s+\bfrom\b", expr)[0]
    e = re.split(r"(?i)\s+(left|inner|right|full)\s+join\b", e)[0]
    return e.strip()

# --------------------- alias + harvesting ---------------------
def find_alias_for_source_fixed(source: str, texts: List[str]) -> str:
    """Ignore trailing ON/WITH so alias never becomes 'ON'."""
    src = source.lower()
    for txt in texts:
        for m in re.finditer(r"(?i)\bfrom\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\b(?!\s+(on|with)\b)", txt):
            tbl, alias = m.group(1), m.group(2)
            if tbl.split(".")[-1].lower() == src:
                return alias
        for m in re.finditer(r"(?i)\bjoin\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\b(?!\s+(on|with)\b)", txt):
            tbl, alias = m.group(1), m.group(2)
            if tbl.split(".")[-1].lower() == src:
                return alias
    return source[:4].lower() if source else "src"

def harvest_identifiers_for_source(src: str, texts: List[str], known_cols: Set[str], alias: str) -> List[str]:
    seen = set()
    out: List[str] = []
    a = alias.lower()
    src_low = src.lower()
    pat = re.compile(rf"\b(?:{re.escape(a)}|{re.escape(src_low)})\.([A-Za-z][A-Za-z0-9_]*)\b")
    for t in texts:
        for col in pat.findall(t):
            if col.lower() not in seen:
                seen.add(col.lower()); out.append(col)
        for tok in re.findall(r"[A-Za-z][A-Za-z0-9_]*", t):
            if tok in known_cols and tok.lower() not in seen:
                seen.add(tok.lower()); out.append(tok)
    return out

def enrich_columns_from_case(src: str, alias: str, texts: List[str], known_cols: Set[str]) -> Set[str]:
    extra = set()
    case_texts = [t for t in texts if "CASE" in t.upper()]
    for txt in case_texts:
        for (qual, col) in QUAL_ID_RX.findall(txt):
            if qual.lower() in (src.lower(), alias.lower()):
                if not re.match(r"^\d+$", col):
                    extra.add(col)
    return set(known_cols).union(extra)

# --------------------- CASE + WHERE extraction ---------------------
def extract_case_and_filter_blocks(texts: List[str]):
    case_blocks, where_blocks = [], []
    for raw in texts:
        t = s(raw)
        if not t:
            continue
        t_norm = re.sub(r"\s+", " ", t)

        if re.search(r"\bCASE\b", t_norm, re.I):
            clean_case = strip_from_join(t_norm)
            segs = re.findall(r"(?is)(CASE .*? END)", clean_case)
            if segs:
                case_blocks.extend([seg.strip() for seg in segs])
            else:
                case_blocks.append(clean_case.strip())
            continue

        if re.search(r"\bWHERE\b", t_norm, re.I):
            cond = re.split(r"(?i)\bwhere\b", t_norm)[-1]
            where_blocks.append(cond.strip())
        elif any(op in t_norm.upper() for op in ["=", "<>", ">", "<", " IN ", " LIKE "]):
            where_blocks.append(t_norm.strip())

    ignore_terms = ["need to know", "check", "entity details", "for info"]
    clean_where, seen = [], set()
    for w in where_blocks:
        wl = w.lower().strip()
        if any(it in wl for it in ignore_terms):  # developer notes
            continue
        if " join " in wl or " with " in wl or " from " in wl:  # join noise
            continue
        if wl not in seen:
            seen.add(wl)
            clean_where.append(w.strip())

    clean_case, seen_case = [], set()
    for c in case_blocks:
        cl = c.lower().strip()
        if cl not in seen_case:
            seen_case.add(cl)
            clean_case.append(c.strip())

    return clean_case, clean_where

# --------------------------- main ---------------------------
def parse_rules(csv_path: str, outdir: str) -> None:
    df = load_csv(csv_path)

    per_source: Dict[str, Dict[str, List[str]]] = {}
    for src in sorted(set(df["src_table"].astype(str).str.strip().str.lower())):
        if not src:
            continue
        sdf = df[df["src_table"].astype(str).str.strip().str.lower() == src]
        texts: List[str] = []
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
        known_cols = enrich_columns_from_case(src, alias, texts, known_cols)
        referenced_cols = harvest_identifiers_for_source(src, texts, known_cols, alias)

        case_blocks, where_blocks = extract_case_and_filter_blocks(texts)

        # Infer SRSTATUS = 'A' if business text implies excluding inactive
        inferred = []
        for t in texts:
            if re.search(r"SRSTATUS\s*<>\s*'A'", t, re.I) or "exclude inactive" in t.lower():
                inferred.append(f"{alias}.SRSTATUS = 'A'")
        where_blocks = sorted(set(where_blocks + inferred))

        interpretation[src] = {
            "alias": alias,
            "known_columns": sorted(list(known_cols)),
            "referenced_columns": sorted(set(referenced_cols)),
            "candidate_where_predicates": where_blocks,
            "case_like_expressions": case_blocks,
        }

    out = Path(outdir); out.mkdir(parents=True, exist_ok=True)
    (out / "nlp_rules_interpretation_v4.json").write_text(json.dumps(interpretation, indent=2))

    # Markdown log
    lines = ["# NLP Parsing Report v4\n"]
    for src, data in interpretation.items():
        lines.append(f"## Source: {src}")
        lines.append(f"- Alias: `{data['alias']}`")
        lines.append(f"- Known columns: {', '.join(data['known_columns']) or '(none)'}")
        lines.append(f"- Referenced columns: {', '.join(data['referenced_columns']) or '(none)'}")
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
    (out / "nlp_rules_interpretation_v4.md").write_text("\n".join(lines))

    print(f"✅ NLP interpretation written to:\n  {out/'nlp_rules_interpretation_v4.json'}\n  {out/'nlp_rules_interpretation_v4.md'}")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Self-contained NLP parser for transformation mapping CSVs (v4).")
    p.add_argument("csv", help="Path to source-target mapping CSV")
    p.add_argument("--outdir", required=True, help="Output directory for results")
    args = p.parse_args()
    parse_rules(args.csv, args.outdir)
