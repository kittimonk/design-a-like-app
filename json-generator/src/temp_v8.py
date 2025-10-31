#!/usr/bin/env python3
"""
extract_sources_columns_v7.py — dynamically infers source-column lineage, join logic,
business rules, derived CASE expressions, and static assignments from the cleaned
source-target mapping CSV and NLP interpretation file.

- No hardcoded alias map (aliases inferred dynamically)
- Cleans freeform business rules / join clauses
- Normalizes static assignments and CASE logic
- Ensures lineage contains only qualified alias.column names
"""

import json, re, math, os
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
import pandas as pd

# ------------------------------------------------------------------------------
# Canonical header mapping
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

QUAL_ID_RX  = re.compile(r"\b([a-z][a-z0-9_]*)\.([a-z][a-z0-9_]*)\b", re.I)

# ------------------------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------------------------
def s(x) -> str:
    if x is None: return ""
    try:
        if isinstance(x, float) and (math.isnan(x) or pd.isna(x)): return ""
    except Exception:
        pass
    return str(x).replace("\r"," ").replace("\n"," ").strip()

def canon_headers(df: pd.DataFrame) -> pd.DataFrame:
    cols = []
    for c in df.columns:
        mapped = None
        for rx, tgt in CANON_MAP.items():
            if re.match(rx, str(c).strip()):
                mapped = tgt; break
        cols.append(mapped or str(c))
    out = df.copy(); out.columns = cols; return out

def _collapse_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns); seen = {}
    for i,c in enumerate(cols):
        seen.setdefault(c, []).append(i)
    out = pd.DataFrame(index=df.index)
    for name, idxs in seen.items():
        if len(idxs)==1:
            out[name] = df.iloc[:, idxs[0]]
        else:
            merged = df.iloc[:, idxs[0]].astype(str)
            for j in idxs[1:]:
                cur = df.iloc[:, j].astype(str)
                merged = merged.where(merged.str.strip()!="", cur)
            out[name] = merged
    return out

def load_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")
    df = canon_headers(df)
    df = _collapse_duplicate_columns(df)
    for c in ["src_table","src_column","tgt_table","tgt_column","business_rule","join_clause","transformation_rule"]:
        if c not in df.columns: df[c] = ""
        df[c] = df[c].astype(str)
    df["src_table"] = df["src_table"].apply(lambda v: (str(v).strip().split()[0].lower() if str(v).strip() else ""))
    df["src_column"] = df["src_column"].str.strip().str.lower()
    df["tgt_column"] = df["tgt_column"].str.strip().str.lower()
    for c in ["business_rule","join_clause","transformation_rule"]:
        df[c] = df[c].apply(lambda x: s(x))
    return df

# ------------------------------------------------------------------------------
# Alias & normalization utilities
# ------------------------------------------------------------------------------
def learn_source_columns(df: pd.DataFrame) -> Dict[str, Set[str]]:
    out: Dict[str, Set[str]] = {}
    for _, row in df.iterrows():
        t = s(row.get("src_table","")).lower()
        c = s(row.get("src_column","")).lower()
        if not t or not c: continue
        if re.match(r"(?i)^t_[a-z0-9_]+_\d+$", c): continue
        if c == "nan": continue
        out.setdefault(t, set()).add(c)
    return out

def dynamic_alias(source: str, texts: List[str]) -> str:
    src = source.lower()
    for txt in texts:
        t = " " + txt + " "
        for m in re.finditer(r"(?i)\b(from|join)\s+([a-z0-9_\.]+)\s+([a-z][a-z0-9_]*)\b", t):
            _, tbl, a = m.groups()
            if tbl.split(".")[-1].lower()==src and a.lower() not in {"on","with","join"}:
                return a.lower()
    base = re.sub(r"[^a-z]+","", src)
    if not base: return "src"
    head = base[0]
    tail = re.sub(r"[aeiou]","", base[1:])
    alias = (head + tail)[:4]
    return alias or (base[:4] if len(base)>=4 else base)

def normalize_join_text(j: str) -> Optional[str]:
    j = s(j)
    if not j or j.lower() in {"nan","none"}: return None
    m = re.search(r"(?is)\bjoin\s+([a-z0-9_\.]+)\s+([a-z][a-z0-9_]*)\s+with\s+([a-z0-9_\.]+)\s+([a-z][a-z0-9_]*)\s+on\s+(.*)$", j, re.I)
    if m:
        _, _, right_tbl, right_alias, on = m.groups()
        return f"left join {right_tbl.lower()} {right_alias.lower()} on {on.strip()}".lower()
    if " join " in j.lower() and " on " not in j.lower():
        j = j + " on 1=1"
    return re.sub(r"\s+"," ", j).strip().lower()

def unique_list(seq: List[str]) -> List[str]:
    out=[]; seen=set()
    for x in seq:
        if not x: continue
        k = re.sub(r"\s+"," ", x.strip().lower())
        if k in seen: continue
        seen.add(k); out.append(x.strip().lower())
    return out

# ------------------------------------------------------------------------------
# Rule parsers & text normalizers
# ------------------------------------------------------------------------------
DEV_NOTE_WORDS = [
    "reject the record", "log an exception", "no exception", "need to know",
    "entity details", "for info", "then match", "if a match found",
    "note:", "format", "straight move"
]

def looks_like_sql_predicate(line: str, alias: str, known_cols: Set[str]) -> bool:
    L = line.lower()
    if any(w in L for w in DEV_NOTE_WORDS): return False
    if " join " in L or " with " in L or " from " in L: return False
    if not re.search(r"(=|<>|>=|<=|>|<| like | in | is null| is not null)", L): return False
    qual_ok = bool(re.search(rf"\b{re.escape(alias.lower())}\.[a-z][a-z0-9_]*\b", L))
    unqual_ok = any(re.search(rf"\b{re.escape(c)}\b", L) for c in known_cols)
    return qual_ok or unqual_ok

def strip_sql_comments(expr: str) -> str:
    return re.sub(r"--.*?$","", expr, flags=re.M).strip()

def strip_from_join(expr: str) -> str:
    e = re.split(r"(?i)\s+\bfrom\b", expr)[0]
    e = re.split(r"(?i)\s+(left|inner|right|full)\s+join\b", e)[0]
    return e.strip()

def clean_case_expression(raw: str) -> str:
    t = strip_sql_comments(raw)
    t = strip_from_join(t)
    segs = re.findall(r"(?is)(case .*? end)", t, re.I)
    if segs:
        return re.sub(r"\s+"," ", segs[0]).strip().lower()
    return re.sub(r"\s+"," ", t).strip().lower()

def normalize_static_value(v: str) -> Optional[str]:
    if not v: return None
    x = s(v).lower()
    x = re.sub(r"\s*\(.*?\)\.?\s*$","", x).strip()
    x = re.sub(r"^set\s+to\s+","", x).strip()
    if "etl.effective.start.date" in x:
        return "to_date('\"\"\"${etl.effective.start.date}\"\"\"', 'yyyymmddhhmmss')"
    if re.search(r"9999-12-31", x):
        return "to_date('9999-12-31', 'yyyy-mm-dd')"
    if x.startswith("current_timestamp"):
        return "current_timestamp()"
    if x in {"null","none",""}:
        return "null"
    m = re.match(r"^\+?0*([0-9]+)$", x)
    if m:
        return str(int(m.group(1)))
    if re.match(r"^'.*'$", x):
        return x
    return x

# ------------------------------------------------------------------------------
# CASE + Static assignments + Business rules
# ------------------------------------------------------------------------------
def map_cases_to_targets(df_src: pd.DataFrame) -> List[Dict[str,str]]:
    out = []
    for _, r in df_src.iterrows():
        tr = s(r.get("transformation_rule",""))
        if not tr or "case" not in tr.lower(): 
            continue
        tgt = s(r.get("tgt_column","")).lower() or "derived_col"
        expr = clean_case_expression(tr)
        out.append({"name": tgt, "expression": expr})
    uniq=[]; seen=set()
    for d in out:
        k=(d["name"], d["expression"])
        if k in seen: continue
        seen.add(k); uniq.append(d)
    return uniq

def collect_static_assignments(df_src: pd.DataFrame) -> List[Dict[str,str]]:
    out=[]
    for _, r in df_src.iterrows():
        tr = s(r.get("transformation_rule",""))
        tgt = s(r.get("tgt_column","")).lower()
        if not tgt or not tr: continue
        if "case" in tr.lower(): continue
        norm = normalize_static_value(tr)
        if norm is not None and norm != "": 
            out.append({"value": norm, "target_column": tgt})
    uniq=[]; seen=set()
    for a in out:
        k=(a["target_column"], a["value"])
        if k in seen: continue
        seen.add(k); uniq.append(a)
    return uniq

def parse_business_rules_to_predicates(df_src: pd.DataFrame, alias: str, known_cols: Set[str]) -> Tuple[List[str], List[str]]:
    parsed_rules=[]; where_preds=[]
    for _, r in df_src.iterrows():
        br = s(r.get("business_rule",""))
        if not br: continue
        frags = re.split(r"\.|\;|\band\b", br, flags=re.I)
        for f in frags:
            f = f.strip()
            if not f: continue
            if looks_like_sql_predicate(f, alias, known_cols):
                where_preds.append(re.sub(r"\s+"," ", f).lower())
            else:
                L = f.lower()
                if re.search(rf"\b{re.escape(alias)}\.[a-z][a-z0-9_]*\b", L) or re.search(r"(duplicate|exclude|reject|trim|<>|=)", L):
                    parsed_rules.append(re.sub(r"\s+"," ", f).lower())
    return unique_list(parsed_rules), unique_list(where_preds)

def parse_join_logic(df_src: pd.DataFrame) -> List[str]:
    joins=[]
    for _, r in df_src.iterrows():
        j = s(r.get("join_clause",""))
        if not j: continue
        joins.append(normalize_join_text(j))
    return unique_list([j for j in joins if j])

def collect_lineage(alias: str, blocks: List[str], known_cols: Set[str]) -> Dict[str, List[str]]:
    lineage=set()
    for b in blocks:
        for q, c in QUAL_ID_RX.findall(b):
            lineage.add(f"{q.lower()}.{c.lower()}")
    for c in known_cols:
        lineage.add(f"{alias}.{c}")
    return {"source_level": sorted(lineage)}

# ------------------------------------------------------------------------------
# Main execution
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Extract source-column lineage and SQL components dynamically.")
    p.add_argument("csv", help="Path to source-target mapping CSV")
    p.add_argument("--nlp_json", help="Path to NLP interpretation JSON", default="")
    p.add_argument("--outdir", help="Output directory", required=True)
    args = p.parse_args()

    df = load_csv(args.csv)
    nlp = {}
    if args.nlp_json and os.path.exists(args.nlp_json):
        with open(args.nlp_json, "r") as f:
            nlp = json.load(f)

    sources = sorted([t for t in df["src_table"].unique() if t])
    src_cols_map = learn_source_columns(df)
    summary = {}

    for src in sources:
        df_src = df[df["src_table"]==src].copy()
        texts = []
        for col in ["join_clause","business_rule","transformation_rule"]:
            texts.extend([s(v) for v in df_src[col].tolist() if s(v)])
        alias = None
        if src in nlp and isinstance(nlp[src], dict):
            alias = s(nlp[src].get("alias",""))
            if alias.lower() in {"on","with","join","ref","ref1","ref2","ref3",""}:
                alias = None
        if not alias:
            alias = dynamic_alias(src, texts)
        alias = alias.lower()

        known_cols = set([c.lower() for c in src_cols_map.get(src, set())])
        case_derived = map_cases_to_targets(df_src)
        join_logic = parse_join_logic(df_src)
        business_rules_parsed, candidate_where = parse_business_rules_to_predicates(df_src, alias, known_cols)
        static_assignments = collect_static_assignments(df_src)

        referenced=set()
        pat = re.compile(rf"\b(?:{re.escape(alias)}|{re.escape(src)})\.([a-z][a-z0-9_]*)\b", re.I)
        for t in texts:
            for col in pat.findall(t):
                referenced.add(col.lower())
            for tok in re.findall(r"[a-z][a-z0-9_]*", t.lower()):
                if tok in known_cols:
                    referenced.add(tok)

        lineage_blocks = join_logic + business_rules_parsed + [d["expression"] for d in case_derived]
        lineage = collect_lineage(alias, lineage_blocks, known_cols)

        summary[src] = {
            "alias": alias,
            "known_columns": sorted(list(known_cols)),
            "referenced_columns": sorted(list(referenced)),
            "candidate_where_predicates": candidate_where,
            "join_logic": join_logic,
            "business_rules": business_rules_parsed,
            "lineage": lineage,
            "derived_columns": case_derived,
            "static_assignments": static_assignments,
        }

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "source_columns_summary_v7.json").write_text(json.dumps(summary, indent=2))

    # Markdown summary
    lines = ["# Source → Columns Summary v7\n"]
    for src, data in summary.items():
        lines.append(f"## {src}")
        lines.append(f"- alias: `{data['alias']}`")
        lines.append(f"- known: {', '.join(data['known_columns']) or '(none)'}")
        lines.append(f"- referenced: {', '.join(data['referenced_columns']) or '(none)'}")
        if data["candidate_where_predicates"]:
            lines.append("### candidate_where_predicates")
            for w in data["candidate_where_predicates"]:
                lines.append(f"- {w}")
        if data["join_logic"]:
            lines.append("### join_logic")
            for j in data["join_logic"]:
                lines.append(f"- {j}")
        if data["business_rules"]:
            lines.append("### business_rules")
            for b in data["business_rules"]:
                lines.append(f"- {b}")
        if data["derived_columns"]:
            lines.append("### derived_columns")
            for d in data["derived_columns"]:
                lines.append(f"- {d['name']}: `{d['expression']}`")
        if data["static_assignments"]:
            lines.append("### static_assignments")
            for a in data["static_assignments"]:
                lines.append(f"- {a['target_column']} := {a['value']}")
        if data["lineage"] and data["lineage"].get("source_level"):
            lines.append("### lineage.source_level")
            for l in data["lineage"]["source_level"]:
                lines.append(f"- {l}")
        lines.append("")
    (outdir / "source_columns_summary_v7.md").write_text("\n".join(lines))
