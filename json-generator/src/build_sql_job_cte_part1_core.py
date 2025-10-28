
from __future__ import annotations

import re
import json
import math
import textwrap
import datetime as _dt
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from difflib import SequenceMatcher
from pathlib import Path


# ==============================
# Logging utilities (file-based)
# ==============================

DEBUG_DIR = Path("json-generator/debug_outputs")
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

def _log(name: str, content: str) -> None:
    p = DEBUG_DIR / name
    with p.open("a", encoding="utf-8") as f:
        ts = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"\n[{ts}] {name}\n" + "-"*80 + "\n")
        f.write(content.rstrip() + "\n")

def _squash(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def _clean_free_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = re.sub(r"(?i)\blog an exception.*", "", s or "")
    return s.strip()


# ==============================
# NLP helpers (lightweight)
# ==============================

_CANON_HEADERS = {
    # Canonical name      : variants commonly seen in your sheets
    "src_table"          : [
        "table/file name * (auto populate)",
        "source table", "src table", "source name", "input table", "table file name * (auto populate)"
    ],
    "src_column"         : ["column name * (auto populate)", "source column", "src column", "input column"],
    "tgt_table"          : ["table/file name * (auto populate).1", "table/file name * (auto populate)__1", "target table", "tgt table"],
    "tgt_column"         : ["column/field name * (auto populate)", "target column", "tgt column", "output column"],
    "tgt_datatype"       : ["data type * (auto populate).1", "data type * (auto populate)__1", "target type", "datatype"],
    "business_rule"      : ["business rule (auto populate)", "business rule", "rules"],
    "join_clause"        : ["join clause (auto populate)", "join", "joins"],
    "transformation_rule": ["transformation rule/logic (auto populate)", "transformation", "logic", "rule"],
    "tgt_path"           : ["db name/outgoing file path * (auto populate)", "target path", "output path"],
    "src_path"           : ["db name/incoming file path *", "source path", "input path"],
}

def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").strip().lower())

def _best_header_match(h: str) -> Optional[str]:
    """Map a raw header to a canonical header using fuzzy similarity."""
    best = None
    best_score = 0.0
    hn = _norm(h)
    for canon, variants in _CANON_HEADERS.items():
        # direct match to canonical key itself
        candidates = [canon] + variants
        for v in candidates:
            score = SequenceMatcher(None, hn, _norm(v)).ratio()
            if score > best_score:
                best_score, best = score, canon
    return best if best_score >= 0.55 else None  # tuned threshold

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    mapping = {}
    for col in df.columns:
        canon = _best_header_match(col)
        mapping[col] = canon or col  # keep unknowns
    df = df.rename(columns=mapping)
    # ensure required keys exist
    for must in ["src_table","src_column","tgt_table","tgt_column","tgt_datatype",
                 "business_rule","join_clause","transformation_rule","tgt_path","src_path"]:
        if must not in df.columns:
            df[must] = ""
    return df

# Simple phrase lexicon to capture intent in free-form text
LEXICON = {
    "reject"   : re.compile(r"(?i)\breject\b"),
    "exclude"  : re.compile(r"(?i)\bexclude\b"),
    "duplicate": re.compile(r"(?i)\bduplicate\b"),
    "active"   : re.compile(r"(?i)\bstatus\b.*\bA\b|not\s+active", re.I),
    "spaces"   : re.compile(r"(?i)\ball\s*spaces\b"),
}

JOIN_RX = re.compile(
    r"(?is)\b(left|inner|right|full)?\s*join\s+([A-Za-z0-9_\.]+)(?:\s+([A-Za-z0-9_]+))?\s+on\s+(.+?)(?=$|\b(left|inner|right|full)?\s*join\b)"
)

CASE_START = re.compile(r"(?is)^\s*case\b")
FROM_SPLIT = re.compile(r"(?is)\bfrom\b")

def extract_case_core(trans_text: str) -> Tuple[str, str]:
    if not trans_text or not isinstance(trans_text, str):
        return ("","")
    txt = trans_text.strip()
    if not txt:
        return ("","")
    if "from" not in txt.lower() and "join" not in txt.lower():
        return (txt, "")
    if not CASE_START.match(txt):
        return (txt, "")
    parts = FROM_SPLIT.split(txt, maxsplit=1)
    if len(parts) == 2:
        core = parts[0].rstrip()
        trailing = "FROM " + parts[1].strip()
        trailing = re.sub(r"\bFROM\s+ossbr_2_1.*", "", trailing, flags=re.I)
        return (core, f"-- Source context preserved: {_squash(trailing)}")
    return (txt, "")

def _guard_suspicious(expr: str) -> str:
    if not isinstance(expr, str):
        return expr
    if re.search(r"(?i)string_agg\s*\(\s*format\s*\(\s*ascii\s*\(", expr or ""):
        short = (expr[:120] + "...") if len(expr) > 120 else expr
        short = short.replace("/*", "/ *").replace("*/", "* /")
        return f"NULL /* unresolved expression guarded: {short} */"
    return expr

def _parse_set_rule(text: str) -> Optional[str]:
    if not text or not isinstance(text, str):
        return None
    t = text.strip()
    tl = t.lower()

    if "set to null" in tl:
        return "NULL"
    if "current_timestamp" in tl:
        return "CURRENT_TIMESTAMP()"
    if "etl.effective.start.date" in tl:
        return "TO_DATE('\"\"\"${etl.effective.start.date}\"\"\"', 'yyyyMMddHHmmss')"

    # numbers or quoted literals after "Set to"
    m = re.search(r"(?i)\bset(?:\s+\w+)?\s+to\s+(.+)$", t)
    if m:
        val = re.sub(r"--.*", "", m.group(1)).strip().rstrip(".")
        val = re.sub(r"\s*\(.*?\)\s*$", "", val).strip()
        if re.fullmatch(r"[+]?0*\d+", val):
            num = re.sub(r"^[+]?0*", "", val) or "0"
            return num
        if re.fullmatch(r"[-+]?\d+(\.\d+)?", val):
            return val
        if re.fullmatch(r"'[^']*'", val):
            return val
        return "'" + val.strip("'\"") + "'"

    # straight move
    if re.search(r"(?i)straight\s*move", tl):
        return "{source_column}"

    # fallback – keep as-is if looks like SQL
    if re.search(r"(?i)\b(case|when|select|join|from)\b", t):
        return t
    if t.upper() == "NULL":
        return "NULL"
    return "'" + t.strip("'\"") + "'" if t else None


# ==============================
# Business Rule → WHERE parser
# ==============================

def business_rules_to_where(text: str) -> str:
    if not text:
        return ""
    raw = _clean_free_text(text)
    items = re.split(r"(?:^\s*\d+\)\s*|\n)+", raw)
    items = [i for i in items if i and i.strip()]
    preds, notes = [], []

    for l in items:
        s = (l or "").strip()
        if not s:
            continue
        if re.search(r"(?i)\bduplicate\b.*\bossbr_2_1\.SRSECCODE\b", s):
            notes.append("-- TODO: Duplicates: enforce ROW_NUMBER() OVER (PARTITION BY mas.SRSECCODE ORDER BY <choose>) = 1")
        if re.search(r"(?i)ossbr_2_1\.SRSECCODE.*all\s+spaces", s):
            preds.append("TRIM(mas.SRSECCODE) <> ''")
        if re.search(r"(?i)ossbr_2_1\.SRSTATUS\s*<>?\s*'A'", s) or re.search(r"(?i)not\s+active", s):
            preds.append("mas.SRSTATUS = 'A'")
        if re.search(r"(?i)\breject the record\b", s):
            notes.append(f"-- NOTE: Evaluate rule -> {s}")
        if re.search(r"(?i)\bexclude the record\b", s):
            notes.append(f"-- NOTE: Exclusion rule -> {s}")

    body = []
    body.extend(notes)
    if preds:
        body.append(" AND ".join(preds))
    return "\n  ".join(body).strip()


# ==============================
# Join normalization & aliasing
# ==============================

def normalize_join(join_text: str) -> str:
    if not join_text or str(join_text).strip().lower() in ("nan","none",""):
        return ""
    s = _clean_free_text(join_text).strip()
    s = re.sub(r"(?i)\bwith\b", " ", s)
    s = re.sub(r"(?i)\binner\s+join\b", "JOIN", s)
    s = re.sub(r"(?i)\bjoin\b", "JOIN", s)
    s = re.sub(r"[;\n]+", " ", s)
    s = re.sub(r"\s+FROM\s+[A-Za-z0-9_\. ]+(?=(\s+(LEFT|INNER|RIGHT|FULL)\s+JOIN\b|\s*$))", "", s, flags=re.I)
    s = re.sub(r"(?i)\bjoin\s+\S+\s+with\s+([A-Za-z0-9_]+)\s+([A-Za-z0-9_]+)", r"LEFT JOIN \1 \2", s)

    m = re.search(r"LEFT JOIN\s+([A-Za-z0-9_\.]+(?:\s+[A-Za-z0-9_]+)*)\s+ON\s+(.+)", s, re.I)
    if m:
        table_block = m.group(1)
        cond = m.group(2)
        parts = table_block.split()
        if len(parts) > 2:
            table_block = " ".join(parts[-2:])
        s = f"LEFT JOIN {table_block} ON {cond}"

    if not re.search(r"(?i)\b(left|right|full)\s+join\b", s):
        s = re.sub(r"(?i)\bjoin\b", "LEFT JOIN", s)

    # lookup heuristic
    if re.search(r"(?i)\b(_ref|_lkp|_xref|_map|_dim)\b", s):
        s = re.sub(r"(?i)\b(inner|right|full)\s+join\b", "LEFT JOIN", s)

    m = re.search(r"LEFT JOIN\s+([A-Za-z0-9_\.]+(?:\s+[A-Za-z0-9_]+)*)\s+ON\s+(.+)", s, re.I)
    if m:
        table_block = m.group(1)
        cond = m.group(2)
        parts = table_block.split()
        if len(parts) > 2:
            table_block = " ".join(parts[-2:])
        s = f"LEFT JOIN {table_block} ON {cond}"

    s_final = _squash(s)
    s_final = re.sub(r"\bFROM\s+ossbr_2_1.*", "", s_final, flags=re.I)
    _log("joins_debug.log", f"NORMALIZED: {s_final}")
    return s_final

JOIN_SIG_RX = re.compile(
    r"(?i)^\s*(left|inner|right|full)\s+join\s+([A-Za-z0-9_\.]+)(?:\s+([A-Za-z0-9_]+))?\s+on\s+(.+)$"
)

# def ensure_unique_join_aliases(joins: List[str], base_alias: str = "mas") -> List[str]:
#     used = {base_alias.lower()}
#     out, seen = [], set()
#     for j in joins:
#         if not j or not isinstance(j, str):
#             continue
#         m = JOIN_SIG_RX.match(j.strip())
#         if not m:
#             sig = re.sub(r"\s+", " ", j.strip().lower())
#             if sig not in seen:
#                 seen.add(sig)
#                 out.append(j)
#             continue
#         _, table, alias, on = m.groups()
#         # strip potential db.schema prefix to pick alias stem
#         table_short = table.split(".")[-1]
#         if not alias or alias.lower() in used:
#             alias_base = "ref" if table_short.lower().startswith("gls") else table_short.lower()[:3] or "r"
#             k = 1
#             alias2 = alias_base
#             while alias2.lower() in used:
#                 alias2 = f"{alias_base}{k}"
#                 k += 1
#             alias = alias2
#         used.add(alias.lower())
#         on_norm = re.sub(r"\s+", " ", on).strip()
#         js = f"LEFT JOIN {table} {alias} ON {on_norm}"
#         sig = re.sub(r"\s+", " ", js.lower())
#         if sig not in seen:
#             seen.add(sig)
#             out.append(js)
#     # add ON 1=1 if missing
#     fixed = []
#     for j in out:
#         fixed.append(j if re.search(r"\bON\b", j, re.I) else j + " ON 1=1 -- auto-added ON clause")
#     return fixed

def ensure_unique_join_aliases(joins: List[str], base_alias: str = "mas") -> List[str]:
    """
    Normalize and de-duplicate JOIN clauses while keeping alias names unique.
    Removes redundant joins that differ only by spacing or case.
    Auto-assigns new aliases if conflicts occur.
    """
    used = {base_alias.lower()}
    out, seen = [], set()
    for j in joins:
        if not j or not isinstance(j, str):
            continue
        # Clean up and standardize join
        j = j.strip()
        # Skip empty or placeholder joins
        if not j or j.lower() in ("nan", "none"):
            continue
        # Normalize case and spacing for deduplication signature
        sig = re.sub(r"\s+", " ", j.lower()).strip()
        if sig in seen:
            continue
        seen.add(sig)

        # Try to parse join signature pattern
        m = JOIN_SIG_RX.match(j)
        if m:
            _, table, alias, on = m.groups()
            table_short = table.split(".")[-1]
            # Generate alias if missing or reused
            if not alias or alias.lower() in used:
                alias_base = "ref" if table_short.lower().startswith("gls") else table_short.lower()[:3] or "r"
                k = 1
                alias2 = alias_base
                while alias2.lower() in used:
                    alias2 = f"{alias_base}{k}"
                    k += 1
                alias = alias2
            used.add(alias.lower())
            on_norm = re.sub(r"\s+", " ", on or "").strip() or "1=1 -- auto-added ON clause"
            js = f"LEFT JOIN {table} {alias} ON {on_norm}"
        else:
            # Fallback if regex fails
            js = j if "ON" in j.upper() else f"{j} ON 1=1 -- auto-added ON clause"
        # Final deduplication guard
        js_sig = re.sub(r"\s+", " ", js.lower()).strip()
        if js_sig not in seen:
            seen.add(js_sig)
            out.append(js)
    return out


# ==============================
# Transformation expression
# ==============================

def transformation_expression(trans: str, target_col: str, src_col: str, target_datatype: Optional[str] = None) -> Tuple[str, Optional[str]]:
    trans = _clean_free_text(trans)
    if not trans:
        return (src_col or "NULL", None)
    if trans.strip().lower().startswith("case"):
        core, trailing = extract_case_core(trans)
        return (core, trailing or None)
    rule_expr = _parse_set_rule(trans)
    if rule_expr:
        return (rule_expr, None)
    return (trans.strip(), None)


# ==============================
# CSV loader w/ robustness
# ==============================

def _rename_dupe_headers(df: pd.DataFrame) -> pd.DataFrame:
    rename_map, seen = {}, {}
    for col in df.columns:
        if col not in seen:
            seen[col] = 0
            rename_map[col] = col
        else:
            seen[col] += 1
            rename_map[col] = f"{col}__{seen[col]}"
    return df.rename(columns=rename_map)

def load_mapping(csv_path: str) -> pd.DataFrame:
    # Flexible read that survives messy quoting or stray separators
    try:
        df = pd.read_csv(csv_path, engine="python")
    except Exception as e1:
        _log("sql_flow_debug.log", f"Primary CSV read failed: {e1!r}. Retrying with sep=None.")
        df = pd.read_csv(csv_path, engine="python", sep=None)
    df = _rename_dupe_headers(df)
    df = normalize_headers(df)

    # Auto-clean JOIN fragments that leaked into transformation_rule
    if "transformation_rule" in df.columns and "join_clause" in df.columns:
        for i, row in df.iterrows():
            tr = str(row.get("transformation_rule","")).strip()
            jc = str(row.get("join_clause","")).strip()
            if re.search(r"(?i)\b(join|from|on)\b", tr):
                join_part_match = re.search(r"(?i)\b(from|join|on)\b.*", tr, re.DOTALL)
                if join_part_match:
                    join_part = join_part_match.group(0)
                    new_jc = (jc + " " + join_part).strip() if jc else join_part
                    df.at[i,"join_clause"] = new_jc
                    tr_clean = re.sub(r"(?i)\b(from|join|on)\b.*", "", tr, flags=re.DOTALL).strip()
                    df.at[i,"transformation_rule"] = tr_clean
                    _log("auto_join_cleanup.log",
                         f"Moved JOIN/FROM from transformation_rule[{i}] to join_clause:\n  OLD: {tr}\n  NEW join_clause: {new_jc}")
    return df.fillna("")


# ==============================
# Per-source CTE builder
# ==============================

def build_source_cte_sql(source: str, df: pd.DataFrame, base_alias: str = "mas") -> Tuple[str, List[Dict[str,str]]]:
    """
    Build a per-source CTE that selects * from the source and applies any
    joins/business rules relevant to rows whose src_table == source.
    Returns (sql_text, audit_rows_for_source).
    """
    # Fully safe filtering
    sdf = df[
        df["src_table"]
        .apply(lambda x: (str(x).strip().split()[0].lower() if str(x).strip() else ""))
        == source.lower()
    ].copy()

    if sdf.empty:
        # If no explicit rows, still create a simple pass-through view from the source
        # Try to use all columns from df that belong to this source if available
        cols = [
            f"{base_alias}.{c.strip()}"
            for c in df.loc[
                df["src_table"].astype(str).str.lower() == source.lower(), "src_column"
            ].dropna().unique()
            if str(c).strip() not in ("", "nan")
        ]
        col_list = ",\n    ".join(cols) if cols else f"{base_alias}.*"
        core = f"SELECT\n    {col_list}\nFROM {source} {base_alias}"
        return core, []

    # Build joins
    joins_raw = []
    for txt in list(sdf.get("join_clause", [])):
        j = normalize_join(txt)
        if j:
            joins_raw.append(j)
    joins = ensure_unique_join_aliases(joins_raw, base_alias=base_alias)

    # Business rules → WHERE
    where_blocks = []
    for txt in list(sdf.get("business_rule", [])):
        blk = business_rules_to_where(txt)
        if blk:
            where_blocks.append(blk)
    where_clause = ""
    if where_blocks:
        deduped = []
        seen = set()
        for b in where_blocks:
            k = b.lower().strip()
            if k and k not in seen:
                deduped.append(b)
                seen.add(k)
        where_clause = "\nWHERE\n  " + "\n  AND ".join(
            [f"-- Business Rule Block #{i+1}\n  {b}" for i, b in enumerate(deduped)]
        )

    # Core select
    core = f"SELECT {base_alias}.*\nFROM {source} {base_alias}"
    if joins:
        core += "\n" + "\n".join(f"{j}" for j in joins)
    if where_clause:
        core += "\n" + where_clause
    return core, []

# ==============================
# Column-level transformation synthesis (for final CTE)
# ==============================

def build_target_select(df: pd.DataFrame, driving_source: str = None) -> Tuple[str, List[Dict[str,str]]]:
    """
    Synthesize the final SELECT list using transformation expressions for each target column.
    Pre-assumes FROM step will be based on dt_<target>_<malcode>_<driving_source> view in Part 2.
    """
    lines, audit = [], []

    # Fix: coerce to string before .str.lower() to avoid AttributeError
    tgt_series = df["tgt_column"].apply(
        lambda x: str(x).lower() if (x is not None and not (isinstance(x, float) and pd.isna(x))) else ""
    )
    grouped = df.reset_index(drop=True).groupby(tgt_series.reset_index(drop=True), dropna=False)


    for tgt_lower, group in grouped:
        tgt = str(group["tgt_column"].iloc[0]).strip()
        tgt_dtype = (str(group["tgt_datatype"].iloc[0]) if "tgt_datatype" in group else "").strip()

        unique_rules = list({(r or "").strip() for r in group.get("transformation_rule", []) if str(r).strip()})
        unique_sources = list({(s or "").strip() for s in group.get("src_column", []) if str(s).strip()})

        merged_note = ""
        if len(unique_rules) > 1:
            merged_note = f"-- NOTE: merged {len(unique_rules)} variations for target column '{tgt}'"
        elif len(group) > 1:
            merged_note = f"-- NOTE: merged {len(group)} duplicate definitions for target column '{tgt}'"

        raw_trans = unique_rules[0] if unique_rules else ""
        src_col = unique_sources[0] if unique_sources else ""

        expr, trailing_comment = transformation_expression(
            raw_trans,
            target_col=tgt,
            src_col=src_col,
            target_datatype=tgt_dtype
        )

        # Remove any FROM/JOIN fragments that slipped into the expression
        expr = re.split(r"(?i)\s+\bfrom\b", expr)[0]
        expr = re.split(r"(?i)\s+(left|inner|right|full)\s+join\b", expr)[0]
        expr = _guard_suspicious(expr).strip()

        # Fix incomplete CASE ... END blocks automatically
        case_count = len(re.findall(r"(?i)\bCASE\b", expr))
        end_count = len(re.findall(r"(?i)\bEND\b", expr))
        if case_count > end_count:
            expr = expr.rstrip() + ("\n  END" * (case_count - end_count))
            _log("sql_validator.log", f"Auto-fixed {case_count - end_count} missing END(s) for CASE in column '{tgt}'")

        # Compute safe alias name for target column
        safe_col = re.sub(r"[^A-Za-z0-9_]", "_", tgt)
        if not re.search(r"(?i)\bas\s+\w+\b\s*$", expr.strip()):
            select_line = f"    {expr} AS {safe_col}"
        else:
            select_line = f"    {expr}"

        if merged_note:
            select_line = f"    {merged_note}\n{select_line}"
        if trailing_comment:
            select_line = f"{select_line}\n    {trailing_comment}"

        lines.append(select_line)
        audit.append({
            "target": tgt,
            "raw": raw_trans.replace("\n", " ").strip(),
            "sql": expr.strip(),
            "note": merged_note or (trailing_comment or ""),
        })

    # Build final SELECT list
    select_sql = "SELECT\n" + ",\n".join(lines)
    return select_sql, audit

# ==============================
# Public API for Part 2
# ==============================

@dataclass
class ParsedMapping:
    df: pd.DataFrame
    sources: List[str]
    target_table: str

def infer_sources(df: pd.DataFrame) -> List[str]:
    vals = []
    for raw in df["src_table"].astype(str):
        raw = raw.strip()
        if not raw:
            continue
        vals.append(raw.split()[0])
    out = []
    for v in vals:
        if v and v not in out:
            out.append(v)
    return out

def infer_target(df: pd.DataFrame) -> str:
    if "tgt_table" in df.columns:
        vals = [str(x).strip() for x in df["tgt_table"].unique() if str(x).strip()]
        return vals[0] if vals else "UNKNOWN"
    return "UNKNOWN"

def parse_mapping(csv_path: str) -> ParsedMapping:
    df = load_mapping(csv_path)
    sources = infer_sources(df)
    target = infer_target(df)
    _log("sql_flow_debug.log", f"Parsed sources: {sources}\nTarget: {target}")
    return ParsedMapping(df=df, sources=sources, target_table=target)

def build_per_source_ctes(pm: ParsedMapping, base_alias: str = "mas") -> Dict[str, str]:
    out = {}
    for s in pm.sources:
        sql, _audit = build_source_cte_sql(s, pm.df, base_alias=base_alias)
        out[s] = sql
        _log("sql_flow_debug.log", f"Built per-source CTE for {s}:\n{sql}")
    return out

def build_final_select_only(pm: ParsedMapping, driving_source: Optional[str] = None) -> Tuple[str, List[Dict[str,str]]]:
    driving = driving_source or (pm.sources[0] if pm.sources else "source")
    select_sql, audit = build_target_select(pm.df, driving_source=driving)
    return select_sql, audit
