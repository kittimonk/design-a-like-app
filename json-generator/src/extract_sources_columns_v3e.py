#!/usr/bin/env python3
import json, re
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd

# ----------------------------
# Canonical column mapping
# ----------------------------
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

AS_ALIAS_RX = re.compile(r"(?i)\bAS\s+([A-Za-z_][A-Za-z0-9_]*)")
QUAL_ID_RX  = re.compile(r"\b([A-Za-z][A-Za-z0-9_]*)\.([A-Za-z][A-Za-z0-9_]*)\b")
JOIN_STD_RX = re.compile(r"(?is)\b(left|right|inner|full)?\s*join\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\s+on\s+(.*)$")

# ----------------------------
# Utility functions
# ----------------------------
def s(x) -> str:
    if x is None:
        return ""
    try:
        if isinstance(x, float) and pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x)

def canon_headers(df: pd.DataFrame) -> pd.DataFrame:
    cols = []
    for c in df.columns:
        mapped = None
        for rx, tgt in CANON_MAP.items():
            if re.match(rx, str(c).strip()):
                mapped = tgt
                break
        cols.append(mapped or str(c))
    out = df.copy()
    out.columns = cols
    return out

def collapse_dupe_columns(df: pd.DataFrame) -> pd.DataFrame:
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
    df = collapse_dupe_columns(df)
    for col in ["src_table","src_column","tgt_table","tgt_column","business_rule","join_clause","transformation_rule"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].astype(str)
    df["src_table"] = df["src_table"].apply(lambda v: (str(v).strip().split()[0] if str(v).strip() else ""))
    return df

def preferred_alias(src_key: str, nlp_alias: str) -> str:
    a = (nlp_alias or src_key[:4]).lower()
    if src_key.startswith("ossbr"): return "mas"
    if src_key == "glsxref": return "glsx"
    if src_key in ("mfspric","mfsp","mfsprec"): return "mfsp"
    if src_key == "mfin": return "mfin"
    if src_key == "tantrum": return "tant"
    return a

def extract_lineage_terms(text: str, known_cols: List[str], src_key: str, alias: str) -> List[str]:
    out = []
    for (qual, col) in QUAL_ID_RX.findall(text or ""):
        out.append(f"{qual}.{col}")
    kn = set([c.lower() for c in known_cols])
    for t in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", text or ""):
        if t.lower() in kn:
            out.append(t)
            out.append(f"{alias}.{t}" if alias else f"{src_key}.{t}")
    seen=set(); res=[]
    for x in out:
        if x not in seen:
            seen.add(x); res.append(x)
    return res

def parse_static_assignment(txt: str, tgt_col: str) -> Optional[Dict[str, Any]]:
    if not isinstance(txt, str) or not txt.strip():
        return None
    m = re.search(r"(?i)\bset\s+to\s+(.+)$", txt.strip())
    if not m:
        return None
    val = m.group(1).strip().strip("'\"`")

    if "etl.effective.start.date" in val.lower():
        val = "TO_DATE('\"\"\"${etl.effective.start.date}\"\"\"', 'yyyymmddhhmmss')"
    elif re.search(r"9999[-_/]12[-_/]31", val):
        val = "TO_DATE('9999-12-31', 'YYYY-MM-DD')"
    elif re.search(r"(?i)\bcurrent[_\s]?timestamp\b", val):
        val = "current_timestamp()"
    else:
        val = re.sub(r"\([^)]*\)", "", val)
        if not re.search(r"\(\)$", val):
            val = re.sub(r"[\.]+$", "", val.strip())
        val = val.strip()
        if re.fullmatch(r"[+\-]?\d+", val):
            sign = ""
            num = val
            if val[0] in "+-":
                sign = "-" if val[0] == "-" else ""
                num = val[1:]
            if re.match(r"^0{2,}\d*$", num):
                num = re.sub(r"^0+", "", num) or "0"
            val = f"{sign}{num}"
    return {"value": val, "target_column": str(tgt_col).lower()}

def collect_join_logic(df: pd.DataFrame, src_key: str) -> List[str]:
    joins = []
    sdf = df[df["src_table"].astype(str).str.lower()==src_key]
    for j in sdf["join_clause"].dropna().unique():
        j = str(j).strip()
        if not j or "join" not in j.lower():
            continue
        m = JOIN_STD_RX.search(j)
        if m:
            typ, tbl, alias, cond = m.groups()
            typ = (typ or "LEFT").upper()
            joins.append(f"{typ} JOIN {tbl} {alias} ON {cond.strip()}")
    deduped = list(dict.fromkeys([re.sub(r"\s+"," ",x.strip()) for x in joins]))
    return deduped

def collect_business_rules(df: pd.DataFrame, src_key: str) -> List[str]:
    brs = []
    sdf = df[df["src_table"].astype(str).str.lower()==src_key]
    for b in sdf["business_rule"].dropna().unique():
        b = str(b).strip()
        if not b or b.lower() in ("nan","none"):
            continue
        b = re.sub(r"^\d+\)\s*", "", b)
        b = re.sub(r"(?i)^rule[:\-]?\s*", "", b)
        brs.append(b)
    return list(dict.fromkeys(brs))

# ----------------------------
# Main function
# ----------------------------
def extract_sources_columns_v3e(csv_path: str, nlp_json: str, out_json: str, out_md: str):
    df = load_csv(csv_path)
    with open(nlp_json, "r") as f:
        nlp_data = json.load(f)

    csv_known: Dict[str, List[str]] = {}
    for _, r in df.iterrows():
        st = s(r.get("src_table")).strip().split()[0].lower()
        sc = s(r.get("src_column")).strip()
        if st and sc and sc.lower() != "nan":
            csv_known.setdefault(st, []).append(sc)
    for k in list(csv_known.keys()):
        seen=set(); arr=[]
        for c in csv_known[k]:
            if c not in seen:
                seen.add(c); arr.append(c)
        csv_known[k] = arr

    summary: Dict[str, Dict[str, Any]] = {}
    sources = sorted(set(list(df["src_table"].astype(str).str.lower()) + list(nlp_data.keys())))

    for src in sources:
        src_key = str(src).strip().lower()
        if not src_key:
            continue

        n = nlp_data.get(src_key, {})
        alias = preferred_alias(src_key, n.get("alias", ""))

        known_cols = list(dict.fromkeys((n.get("known_columns") or []) + (csv_known.get(src_key, []) or [])))
        referenced_cols = n.get("referenced_columns", [])
        where_preds = n.get("candidate_where_predicates", [])
        case_exprs = n.get("case_like_expressions", [])

        joins = collect_join_logic(df, src_key)
        brules = collect_business_rules(df, src_key)

        static_assigns: List[Dict[str,Any]] = []
        sdf = df[df["src_table"].astype(str).str.lower()==src_key]
        for _, row in sdf.iterrows():
            obj = parse_static_assignment(s(row.get("transformation_rule","")), s(row.get("tgt_column","")))
            if obj: static_assigns.append(obj)

        derived = []
        for i, expr in enumerate(case_exprs, start=1):
            m = AS_ALIAS_RX.search(expr)
            alias_name = m.group(1).lower() if m else f"case_{i}"
            derived.append({"name": alias_name, "expression": expr})

        # lineage enrichment
        lineage_terms = []
        for rc in referenced_cols:
            lineage_terms.append(rc)
            lineage_terms.append(f"{alias}.{rc}" if alias else f"{src_key}.{rc}")
        for lst in [case_exprs, where_preds, joins, brules]:
            for txt in lst:
                lineage_terms += extract_lineage_terms(txt, known_cols, src_key, alias)
        seen=set(); lineage_ordered=[]
        for x in lineage_terms:
            if x not in seen:
                seen.add(x); lineage_ordered.append(x)

        summary[src_key] = {
            "alias": alias,
            "known_columns": known_cols,
            "referenced_columns": referenced_cols,
            "candidate_where_predicates": where_preds,
            "join_logic": joins,
            "business_rules": brules,
            "lineage": { "source_level": lineage_ordered },
            "derived_columns": derived,
            "static_assignments": static_assigns
        }

    Path(out_json).write_text(json.dumps(summary, indent=2))
    # MD summary
    lines = ["# Source & Column Extraction Report (v3e)\n"]
    for src, data in summary.items():
        lines.append(f"## Source: `{src}` (alias: `{data['alias']}`)")
        lines.append(f"- Known: {', '.join(data['known_columns']) or '(none)'}")
        if data["join_logic"]:
            lines.append("- Joins:")
            for j in data["join_logic"][:3]:
                lines.append(f"  - `{j}`")
        if data["business_rules"]:
            lines.append("- Business Rules:")
            for b in data["business_rules"][:3]:
                lines.append(f"  - `{b}`")
        if data["lineage"]["source_level"]:
            lines.append(f"- Lineage sample: {', '.join(data['lineage']['source_level'][:8])}")
        lines.append("")
    Path(out_md).write_text("\n".join(lines))
    return summary

if __name__ == "__main__":
    csv_path = "source_target_mapping_clean_v9_fixed (3).csv"
    nlp_json = "nlp_rules_interpretation_v6.json"
    extract_sources_columns_v3e(csv_path, nlp_json, "source_columns_summary_v3e.json", "source_columns_summary_v3e.md")
