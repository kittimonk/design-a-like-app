#!/usr/bin/env python3
"""
common_utils.py — shared helpers for SQL-job generation scripts.
Merges advanced regex/alias logic with new robust CSV-handling.
"""

import re
from typing import Dict, List, Optional, Set, Tuple
import pandas as pd

# ------------------------------------------------------------------------------
# Canonical column normalization
# ------------------------------------------------------------------------------
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
JOIN_WITH_RX = re.compile(
    r"(?is)\bjoin\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\s+with\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\s+on\s+(.*)$"
)
JOIN_STD_RX  = re.compile(
    r"(?is)\b(left|right|inner|full)?\s*join\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\s+on\s+(.*)$"
)

# ------------------------------------------------------------------------------
# Basic text cleaner
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# Header normalization and duplicate collapsing
# ------------------------------------------------------------------------------
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
    """Collapse duplicate canonical columns by taking first non-empty value across duplicates."""
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
    """Safely read mapping CSV, normalize headers, collapse duplicates, and enforce schema."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")
    df = canon_headers(df)
    df = _collapse_duplicate_columns(df)

    # Ensure expected columns exist and string type
    expected_cols = [
        "src_table", "src_column", "tgt_table", "tgt_column",
        "business_rule", "join_clause", "transformation_rule"
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype(str)

    # Normalize src_table (lowercase, trimmed)
    df["src_table"] = df["src_table"].apply(
        lambda v: str(v).strip().split()[0].lower() if str(v).strip() else ""
    )

    return df

# ------------------------------------------------------------------------------
# Identifier normalization
# ------------------------------------------------------------------------------
def norm_id(x: str) -> str:
    x = s(x).strip().replace("-", "_")
    return x

# ------------------------------------------------------------------------------
# Source-column learning
# ------------------------------------------------------------------------------
def learn_source_columns(df: pd.DataFrame) -> Dict[str, Set[str]]:
    by_src: Dict[str, Set[str]] = {}
    for _, row in df.iterrows():
        t = norm_id(row.get("src_table", "")).lower()
        c = norm_id(row.get("src_column", ""))
        if not t or not c:
            continue
        if re.match(r"(?i)^t_[a-z0-9_]+_\d+$", c):
            continue
        if c.lower() == "nan":
            continue
        by_src.setdefault(t, set()).add(c)
    return by_src

# ------------------------------------------------------------------------------
# Alias and join inference
# ------------------------------------------------------------------------------
def find_alias_for_source(source: str, texts: List[str]) -> Optional[str]:
    src = source.lower()
    for txt in texts:
        for m in re.finditer(r"(?i)\bfrom\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)", txt):
            tbl, alias = m.group(1), m.group(2)
            if tbl.split(".")[-1].lower() == src:
                return alias
        for m in re.finditer(r"(?i)\bjoin\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)", txt):
            tbl, alias = m.group(1), m.group(2)
            if tbl.split(".")[-1].lower() == src:
                return alias
    return None


def harvest_identifiers_for_source(
    source: str, texts: List[str], known_cols: Set[str], alias: Optional[str]
) -> List[str]:
    src = source.lower()
    a = alias.lower() if alias else None
    picked, seen = [], set()
    for txt in texts:
        for (qual, col) in QUAL_ID_RX.findall(txt):
            if (a and qual.lower() == a) or (qual.lower() == src):
                coln = norm_id(col)
                if coln and coln not in seen:
                    seen.add(coln)
                    picked.append(coln)
        for tok in re.findall(r"[A-Za-z][A-Za-z0-9_]*", txt):
            tn = norm_id(tok)
            if tn in known_cols and tn not in seen:
                seen.add(tn)
                picked.append(tn)
    return picked


def normalize_join_text(j: str) -> Optional[str]:
    j = s(j).strip()
    if not j or j.lower() in ("nan", "none"):
        return None
    m = JOIN_WITH_RX.search(j)
    if m:
        _, _, right_tbl, right_alias, on = m.groups()
        return f"LEFT JOIN {right_tbl} {right_alias} ON {on.strip()}"
    if " join " in j.lower() and " on " not in j.lower():
        j = j + " ON 1=1"
    return j


def unique_joins(joins: List[str], base_alias: str) -> List[str]:
    out = []
    seen = set()
    for j in joins:
        if not j:
            continue
        sig = re.sub(r"\s+", " ", j.strip().lower())
        sig = re.sub(r"\bref\d+\b", "ref", sig)
        if sig in seen:
            continue
        seen.add(sig)
        out.append(j)
    return out


def strip_from_join(expr: str) -> str:
    e = re.split(r"(?i)\s+\bfrom\b", expr)[0]
    e = re.split(r"(?i)\s+(left|inner|right|full)\s+join\b", e)[0]
    return e.strip()

# ✅ End of common_utils.py
