#!/usr/bin/env python3
import argparse, json, re
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd

# ---------------- Canonical header mapping ----------------
CANON_MAP = {
    r"(?i)^source schema id.*$": "src_row_id",
    r"(?i)^db name/incoming file path.*$": "src_db",
    r"(?i)^schema name.*auto.*$": "src_schema",
    r"(?i)^table/file name.*auto.*$": "src_table",
    r"(?i)^column name.*auto.*$": "src_column",
    r"(?i)^data type.*auto.*$": "src_datatype",

    r"(?i)^target schema id.*$": "tgt_row_id",
    r"(?i)^db name/outgoing file path.*$": "tgt_db",
    r"(?i)^schema name.*auto.*\.\d+$": "tgt_schema",
    r"(?i)^table/file name.*auto.*\.\d+$": "tgt_table",
    r"(?i)^column/field name.*auto.*$": "tgt_column",
    r"(?i)^data type.*auto.*\.\d+$": "tgt_datatype",

    r"(?i)^business rule.*$": "business_rule",
    r"(?i)^join clause.*$": "join_clause",
    r"(?i)^transformation rule/logic.*$": "transformation_rule",
}

JOIN_STD_RX  = re.compile(r"(?is)\b(left|right|inner|full)?\s*join\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\s+on\s+(.+)$")
FROM_RX      = re.compile(r"(?is)\bfrom\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\b")
JOIN_ANY_RX  = re.compile(r"(?is)\bjoin\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\b")
QUAL_ID_RX   = re.compile(r"\b([A-Za-z][A-Za-z0-9_]*)\.([A-Za-z][A-Za-z0-9_]*)\b")
AS_ALIAS_RX  = re.compile(r"(?i)\bas\s+([A-Za-z_][A-Za-z0-9_]*)\b")

def s(x) -> str:
    if x is None: return ""
    try:
        if isinstance(x, float) and pd.isna(x): return ""
    except Exception:
        pass
    return str(x)

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

def collapse_dupes(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns)
    groups: Dict[str, List[int]] = {}
    for i, c in enumerate(cols):
        groups.setdefault(c, []).append(i)
    out = pd.DataFrame(index=df.index)
    for name, idxs in groups.items():
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
    df = collapse_dupes(df)
    for c in ["src_table","src_column","tgt_table","tgt_column","business_rule","join_clause","transformation_rule"]:
        if c not in df.columns: df[c] = ""
        df[c] = df[c].astype(str)
    # normalize
    df["src_table"] = df["src_table"].apply(lambda v: (str(v).strip().split()[0].lower() if str(v).strip() else ""))
    for c in ["src_column","tgt_column"]:
        df[c] = df[c].apply(lambda z: str(z).strip().lower() if str(z).strip() else "")
    return df

# ---------------- Alias learning ----------------
def learn_aliases(texts: List[str]) -> Dict[str, str]:
    """Return {table_lower: alias_lower} learned from FROM/JOIN patterns across texts."""
    res: Dict[str, str] = {}
    for t in texts:
        if not t: continue
        for m in FROM_RX.finditer(t):
            tbl, alias = m.groups()
            key = tbl.split(".")[-1].lower()
            if key and alias and key not in res:
                res[key] = alias.lower()
        for m in JOIN_ANY_RX.finditer(t):
            tbl, alias = m.groups()
            key = tbl.split(".")[-1].lower()
            if key and alias and key not in res:
                res[key] = alias.lower()
    return res

def unique_alias(base: str, used: set) -> str:
    base = re.sub(r"[^a-z0-9_]", "", base.lower()) or "t"
    if base not in used:
        used.add(base); return base
    i = 1
    while f"{base}{i}" in used:
        i += 1
    used.add(f"{base}{i}")
    return f"{base}{i}"

# ---------------- Static value parsing ----------------
def _normalize_numeric_literal(val: str) -> str:
    v = re.sub(r"\([^)]*\)", "", val).strip()
    v = re.sub(r"\s+\.$", "", v)
    v = re.sub(r"\.+$", "", v)
    if re.fullmatch(r"[+\-]?\d+", v):
        sign = "-" if v[0] == "-" else ""
        num = v[1:] if v[0] in "+-" else v
        if re.match(r"^0{2,}\d*$", num):
            num = re.sub(r"^0+", "", num) or "0"
        v = f"{sign}{num}"
    return v

STATIC_PATTERNS = re.compile(
    r"(?is)\b(set|assign|value|default)\s*(to|as|=)\s*(.+)$"
)

def parse_static_assignment(txt: str, tgt_col: str) -> Optional[Dict[str, Any]]:
    if not isinstance(txt, str) or not txt.strip(): return None
    m = STATIC_PATTERNS.search(txt.strip())
    if not m: return None
    val = m.group(3).strip().strip("'\"`")
    low = val.lower()

    # canonical transforms
    if "etl.effective.start.date" in low:
        val = "to_date('\"\"\"${etl.effective.start.date}\"\"\"', 'yyyymmddhhmmss')"
    elif re.search(r"9999[-_/]12[-_/]31", low):
        val = "to_date('9999-12-31', 'yyyy-mm-dd')"
    elif re.search(r"(?i)\bcurrent[_\s]?timestamp\b", low):
        val = "current_timestamp()"
    else:
        # simple letters like 'n','y','a' keep quoted lower
        if re.fullmatch(r"[a-z]", val, flags=re.I):
            val = f"'{val.lower()}'"
        else:
            val = _normalize_numeric_literal(val)
    return {"value": val, "target_column": str(tgt_col).lower()}

# ---------------- Business rules → SQL ----------------
def business_rule_to_sql(text: str, default_alias: str) -> Optional[str]:
    if not text or not text.strip(): return None
    t = text.strip().lower()

    # duplicates
    if re.search(r"\breject\b.*\bduplicate\b.*\b([a-z_][a-z0-9_]*)\b", t):
        col = re.search(r"\breject\b.*\bduplicate\b.*\b([a-z_][a-z0-9_]*)\b", t).group(1)
        return f"-- remove duplicates based on {default_alias}.{col}"

    # status active
    if re.search(r"status[^a-z0-9]*<>[^a-z0-9]*'a'", t) or re.search(r"status[^a-z0-9]*not\s*=\s*'a'", t):
        return f"where {default_alias}.srstatus = 'a'"

    # all spaces reject
    if re.search(r"\b(all\s+spaces|blank|empty)\b", t) and "srseccode" in t:
        return f"where trim({default_alias}.srseccode) <> ''"

    # generic "exclude" / "include only"
    if "exclude" in t and "where" not in t:
        # heuristic: no exact column → comment
        return f"-- exclusion rule: {t}"
    if "include only" in t:
        return f"-- inclusion rule: {t}"

    # sb fund + mfsp match => don't extract (leave as comment for final stage filter)
    if "mutual fund" in t or "sbb" in t or "mfspric" in t:
        return f"-- special mfsp/gbx filter: {t}"

    # fallback: return comment
    return f"-- rule: {t}"

# ---------------- CASE → derived ----------------
def build_derived_from_cases(texts: List[str]) -> List[Dict[str, str]]:
    out=[]
    for i, expr in enumerate(texts, start=1):
        ex = expr.strip()
        m = AS_ALIAS_RX.search(ex)
        alias = m.group(1).lower() if m else f"case_{i}"
        out.append({"name": alias, "expression": ex.lower()})
    return out

# ---------------- Lineage ----------------
def lineage_from_text(text: str, known: List[str], alias: str, src_key: str) -> List[str]:
    if not text: return []
    known_set = {k.lower() for k in known}
    buf = []

    for (q, c) in QUAL_ID_RX.findall(text):
        buf.append(f"{q.lower()}.{c.lower()}")

    for tok in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", text):
        t = tok.lower()
        if t in known_set:
            buf.append(t)
            buf.append(f"{alias}.{t}")

    # unique in order
    seen=set(); res=[]
    for x in buf:
        if x not in seen:
            seen.add(x); res.append(x)
    return res

# ---------------- Main extraction ----------------
def extract_sources_columns(csv_path: str, nlp_path: str, out_json: str, out_md: str):
    df = load_csv(csv_path)

    nlp = {}
    if nlp_path and Path(nlp_path).exists():
        nlp = json.loads(Path(nlp_path).read_text())

    # collect all texts to learn aliases
    texts_all: List[str] = []
    for c in ["join_clause","transformation_rule","business_rule"]:
        if c in df.columns:
            ser = df[c]
            if isinstance(ser, pd.DataFrame): ser = ser.iloc[:,0]
            texts_all.extend([s(v) for v in ser if s(v)])

    learned_aliases = learn_aliases(texts_all)
    used_aliases = set(learned_aliases.values())

    # enumerate sources
    sources = sorted(set(list(df["src_table"].unique()) + list(nlp.keys())))
    out: Dict[str, Any] = {}
    md_lines = ["# Source & Column Extraction Report (v4)\n"]

    for src in sources:
        if not src: continue

        # alias (prefer learned, else fallback to short unique)
        alias = learned_aliases.get(src)
        if not alias:
            alias = unique_alias(src[:4] if len(src) >= 3 else src, used_aliases)

        # known columns: csv + nlp
        csv_cols = [c.strip().lower() for c in df.loc[df["src_table"]==src, "src_column"].dropna().tolist() if c.strip()]
        nlp_cols = [str(x).strip().lower() for x in (nlp.get(src, {}).get("known_columns") or []) if str(x).strip()]
        known_cols = []
        seen=set()
        for k in (nlp_cols + csv_cols):
            if k and k not in seen:
                seen.add(k); known_cols.append(k)

        # referenced / cases / joins / business
        n_src = nlp.get(src, {})
        referenced = [str(x).strip().lower() for x in (n_src.get("referenced_columns") or []) if str(x).strip()]
        case_texts = [str(x).strip() for x in (n_src.get("case_like_expressions") or []) if str(x).strip()]
        derived = build_derived_from_cases(case_texts)

        # join logic: accept only sql-joins from csv; lowercased + normalized spaces
        joins = []
        sdf = df[df["src_table"]==src]
        for j in sdf["join_clause"].dropna().unique():
            jj = str(j).strip()
            m = JOIN_STD_RX.search(jj)
            if m:
                typ, tbl, als, cond = m.groups()
                typ = (typ or "LEFT").lower()
                joins.append(re.sub(r"\s+"," ", f"{typ} join {tbl.lower()} {als.lower()} on {cond}"))
        joins = list(dict.fromkeys(joins))

        # business rules → sql
        br_sql = []
        # prefer NLP candidate predicates too
        for w in n_src.get("candidate_where_predicates") or []:
            br_sql.append(str(w).strip().lower())
        # plus freeform business rule column:
        for b in sdf["business_rule"].dropna().unique():
            sql = business_rule_to_sql(str(b), alias)
            if sql: br_sql.append(sql)
        # dedupe preserving order
        br_sql = list(dict.fromkeys([re.sub(r"\s+"," ", x.strip()) for x in br_sql if x.strip()]))

        # static assignments from transformation_rule
        static_assigns = []
        for _, row in sdf.iterrows():
            obj = parse_static_assignment(s(row.get("transformation_rule","")), s(row.get("tgt_column","")))
            if obj: static_assigns.append(obj)

        # lineage
        lineage_terms: List[str] = []
        for group in [referenced, case_texts, joins, br_sql]:
            for txt in group:
                lineage_terms += lineage_from_text(str(txt), known_cols, alias, src)
        # unique
        seen=set(); lineage=[]
        for x in lineage_terms:
            if x and x not in seen:
                seen.add(x); lineage.append(x)

        out[src] = {
            "alias": alias,
            "known_columns": known_cols,
            "referenced_columns": referenced,
            "candidate_where_predicates": [],  # predicates are merged into business_rules (sql) now
            "join_logic": joins,
            "business_rules": br_sql,
            "lineage": {"source_level": lineage},
            "derived_columns": derived,
            "static_assignments": static_assigns
        }

        # MD
        md_lines.append(f"## Source: `{src}` (alias: `{alias}`)")
        if known_cols:
            md_lines.append(f"- known: {', '.join(known_cols)}")
        if joins:
            md_lines.append("- joins:")
            for jn in joins: md_lines.append(f"  - `{jn}`")
        if br_sql:
            md_lines.append("- business rules (sql):")
            for br in br_sql: md_lines.append(f"  - `{br}`")
        if derived:
            md_lines.append("- derived columns:")
            for d in derived: md_lines.append(f"  - {d['name']} := case ...")
        if static_assigns:
            md_lines.append("- static assignments:")
            for sa in static_assigns:
                md_lines.append(f"  - {sa['target_column']} := {sa['value']}")
        if lineage:
            md_lines.append(f"- lineage sample: {', '.join(lineage[:8])}")
        md_lines.append("")

    Path(out_json).write_text(json.dumps(out, indent=2))
    Path(out_md).write_text("\n".join(md_lines))

def _cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", help="path to source_target_mapping_clean_v9_fixed (x).csv")
    ap.add_argument("nlp_json", help="path to nlp_rules_interpretation_v6.json")
    ap.add_argument("--out-json", default="source_columns_summary_v4.json")
    ap.add_argument("--out-md", default="source_columns_summary_v4.md")
    args = ap.parse_args()
    extract_sources_columns(args.csv, args.nlp_json, args.out_json, args.out_md)

if __name__ == "__main__":
    _cli()
