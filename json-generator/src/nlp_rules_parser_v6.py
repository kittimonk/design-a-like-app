#!/usr/bin/env python3
import argparse, json, re
from typing import Dict, List, Optional, Set, Tuple
import pandas as pd
from pathlib import Path

# ---------------- Canonicalization helpers ----------------

CANON_MAP = {
    r"(?i)^source schema id.*$": "src_schema_id",
    r"(?i)^source db name/incoming file path.*$": "src_db",
    r"(?i)^source schema name.*auto.*$": "src_schema",
    r"(?i)^source table/file name.*auto.*$": "src_table",
    r"(?i)^source column name.*auto.*$": "src_column",
    r"(?i)^source data type.*auto.*$": "src_datatype",
    r"(?i)^target schema id.*$": "tgt_schema_id",
    r"(?i)^target db name/outgoing file path.*$": "tgt_db",
    r"(?i)^target schema name.*auto.*\.\d+$": "tgt_schema",
    r"(?i)^target table/file name.*auto.*\.\d+$": "tgt_table",
    r"(?i)^target column/field name.*auto.*$": "tgt_column",
    r"(?i)^target data type.*auto.*\.\d+$": "tgt_datatype",
    r"(?i)^business rule.*$": "business_rule",
    r"(?i)^join clause.*$": "join_clause",
    r"(?i)^transformation rule/logic.*$": "transformation_rule",
}

QUAL_ID_RX  = re.compile(r"\b([A-Za-z][A-Za-z0-9_]*)\.([A-Za-z][A-Za-z0-9_]*)\b")

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

def canon_headers(df: pd.DataFrame) -> pd.DataFrame:
    cols = []
    for c in df.columns:
        mapped = None
        for rx, tgt in CANON_MAP.items():
            if re.match(rx, str(c).strip()):
                mapped = tgt; break
        cols.append(mapped or str(c))
    out = df.copy()
    out.columns = cols
    return out

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
    for c in ["src_table","src_column","tgt_table","tgt_column","business_rule","join_clause","transformation_rule"]:
        if c not in df.columns: df[c] = ""
        df[c] = df[c].astype(str)
    # normalize src_table: first token, lowercase
    df["src_table"] = df["src_table"].apply(lambda v: str(v).strip().split()[0].lower() if str(v).strip() else "")
    return df

def learn_source_columns(df: pd.DataFrame) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}
    for _, row in df.iterrows():
        t = str(row.get("src_table","")).strip().lower()
        c = str(row.get("src_column","")).strip()
        if not t or not c:
            continue
        # ignore misfiled target-id tokens
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

# ---------------- Alias handling ----------------

DEFAULT_ALIASES = {
    "ossbr_2_1": "mas",
    "glsxref": "glsx",
    "mfspric": "mfsp",
    "mfin": "mfin",
    "tantrum": "tant",
}

def prefer_default_if_generic(src: str, alias: str) -> str:
    if alias.lower() in {"ref", "ref1", "ref2", "ref3"}:
        return DEFAULT_ALIASES.get(src.lower(), alias.lower())
    return alias.lower()

def find_alias_for_source_v6(source: str, texts: List[str]) -> str:
    """Prefer alias from text; if it's too generic (ref/ref1/...), swap to deterministic default."""
    src = source.lower()
    for txt in texts:
        norm = re.sub(r"\s+", " ", txt.strip())
        # FROM tbl alias
        for m in re.finditer(r"(?i)\bfrom\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\b", norm):
            tbl, alias = m.group(1), m.group(2)
            if alias.lower() in ("on","with","join"):
                continue
            if tbl.split(".")[-1].lower() == src:
                return prefer_default_if_generic(src, alias)
        # JOIN tbl alias
        for m in re.finditer(r"(?i)\bjoin\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\b", norm):
            tbl, alias = m.group(1), m.group(2)
            if alias.lower() in ("on","with","join"):
                continue
            if tbl.split(".")[-1].lower() == src:
                return prefer_default_if_generic(src, alias)
    # fallback deterministic
    return DEFAULT_ALIASES.get(src, (src[:4] if src else "src")).lower()

# ---------------- Identifier harvesting ----------------

def harvest_identifiers_for_source(src: str, texts: List[str], known_cols: Set[str], alias: str) -> List[str]:
    seen = set()
    out: List[str] = []
    a = alias.lower(); src_low = src.lower()
    pat = re.compile(rf"\b(?:{re.escape(a)}|{re.escape(src_low)})\.([A-Za-z][A-Za-z0-9_]*)\b")
    for t in texts:
        for col in pat.findall(t):
            if col.lower() not in seen:
                seen.add(col.lower()); out.append(col)
        # unqualified tokens that match known cols
        for tok in re.findall(r"[A-Za-z][A-Za-z0-9_]*", t):
            if tok in known_cols and tok.lower() not in seen:
                seen.add(tok.lower()); out.append(tok)
    return out

def enrich_columns_from_case(src: str, alias: str, texts: List[str], known_cols: Set[str]) -> Set[str]:
    extra = set()
    for txt in [t for t in texts if "CASE" in t.upper()]:
        for (qual, col) in QUAL_ID_RX.findall(txt):
            if qual.lower() in (src.lower(), alias.lower()) and not re.match(r"^\d+$", col):
                extra.add(col)
    return set(known_cols).union(extra)

# ---------------- CASE/WHERE extraction with noise filtering ----------------

DEV_NOTE_WORDS = [
    "reject the record", "log an exception", "no exception", "need to know",
    "entity details", "for info", "then match", "if a match found", "note:", "format"
]

def looks_like_sql_predicate(line: str, alias: str, known_cols: Set[str]) -> bool:
    L = line.lower()
    if any(w in L for w in DEV_NOTE_WORDS): return False
    if " join " in L or " with " in L or " from " in L: return False
    # must contain an operator
    if not re.search(r"\b(=|<>|>=|<=|>|<| like | in | is null| is not null)\b", L):
        return False
    # must reference alias.col or a known column token
    qual_ok = bool(re.search(rf"\b{re.escape(alias.lower())}\.[A-Za-z][A-Za-z0-9_]*\b", L))
    unqual_ok = any(re.search(rf"\b{re.escape(col.lower())}\b", L) for col in known_cols)
    return qual_ok or unqual_ok

def extract_case_and_filter_blocks_v6(texts: List[str], alias: str, known_cols: Set[str]):
    case_blocks, where_blocks = [], []
    for raw in texts:
        t = s(raw)
        if not t: continue
        t_norm = re.sub(r"\s+", " ", t)

        # CASE harvesting
        if re.search(r"\bCASE\b", t_norm, re.I):
            clean_case = strip_from_join(t_norm)
            segs = re.findall(r"(?is)(CASE .*? END)", clean_case)
            if segs: case_blocks.extend([seg.strip() for seg in segs])
            else: case_blocks.append(clean_case.strip())
            continue

        # WHERE or predicate-like phrases (strict)
        parts = re.split(r"(?i)\bwhere\b", t_norm)
        if len(parts) > 1:
            cond = parts[-1].strip()
            if looks_like_sql_predicate(cond, alias, known_cols):
                where_blocks.append(cond)
        else:
            for frag in re.split(r"\.|\;|\band\b", t_norm, flags=re.I):
                frag = frag.strip()
                if looks_like_sql_predicate(frag, alias, known_cols):
                    where_blocks.append(frag)

    # dedup
    clean_where, seen = [], set()
    for w in where_blocks:
        wl = w.lower().strip()
        if wl not in seen:
            seen.add(wl); clean_where.append(w.strip())

    clean_case, seen_case = [], set()
    for c in case_blocks:
        cl = c.lower().strip()
        if cl not in seen_case:
            seen_case.add(cl); clean_case.append(c.strip())

    return clean_case, clean_where

# ---------------- Main parse ----------------

def parse_rules(csv_path: str, outdir: str) -> Dict[str, Dict]:
    df = load_csv(csv_path)

    per_source: Dict[str, Dict[str, List[str]]] = {}
    for src in sorted(set(df["src_table"].astype(str).str.strip().str.lower())):
        if not src: continue
        sdf = df[df["src_table"].astype(str).str.strip().str.lower() == src]
        texts: List[str] = []
        for c in ["join_clause","business_rule","transformation_rule"]:
            if c in sdf.columns:
                ser = sdf[c]
                if isinstance(ser, pd.DataFrame): ser = ser.iloc[:,0]
                texts.extend([s(v) for v in ser if s(v)])
        per_source[src] = {"texts": texts}

    src_cols_map = learn_source_columns(df)
    interpretation: Dict[str, Dict] = {}

    for src, bundle in per_source.items():
        texts = bundle["texts"]
        alias = find_alias_for_source_v6(src, texts)
        known_cols = src_cols_map.get(src.lower(), set())
        known_cols = enrich_columns_from_case(src, alias, texts, known_cols)
        referenced_cols = harvest_identifiers_for_source(src, texts, known_cols, alias)
        case_blocks, where_blocks = extract_case_and_filter_blocks_v6(texts, alias, known_cols)

        # derive mas.SRSTATUS = 'A' when we see "exclude inactive"/"<> 'A'"
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
    (out / "nlp_rules_interpretation_v6.json").write_text(json.dumps(interpretation, indent=2))

    # Also emit a quick markdown for eyeballing
    lines = ["# NLP Parsing Report v6\n"]
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
                lines.append("```sql"); lines.append(e); lines.append("```")
        lines.append("")
    (out / "nlp_rules_interpretation_v6.md").write_text("\n".join(lines))

    return interpretation

# ---------------- CLI ----------------

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="NLP parser for dev free-text joins/filters/cases.")
    p.add_argument("csv", help="Path to source-target mapping CSV")
    p.add_argument("--outdir", required=True, help="Output directory")
    args = p.parse_args()
    parse_rules(args.csv, args.outdir)
