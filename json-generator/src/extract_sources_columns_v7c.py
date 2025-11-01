#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_sources_columns_v7c.py
Fixes over v7:
 - Default JOIN → LEFT JOIN unless explicitly INNER.
 - Remove lineage noise tokens (e.g., "i.e").
 - De-duplicate derived columns by (name, expression).
 - Normalize static assignments:
    * "set a to a --1a" → "'A'"
    * last_change_dt with etl.effective.start.date → to_date('\"\"\"${etl.effective.start.date}\"\"\"','yyyyMMddHHmmss')
    * to_dt → to_date('9999-12-31','yyyy-MM-dd')
    * "straight move" → alias.src_column (or TO_DATE(...) if target is date-like).
"""
import argparse, json, re
from pathlib import Path
from typing import Dict, List, Optional, Set
import pandas as pd

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

QUAL_ID_RX = re.compile(r"\b([A-Za-z][A-Za-z0-9_]*)\.([A-Za-z][A-Za-z0-9_]*)\b")
GENERIC_BAD_ALIASES = {"on", "with", "join", "as", "using", "ref", "ref1", "ref2", "ref3", "ref4"}

def s(x) -> str:
    if x is None: return ""
    try:
        import math, pandas as pd
        if isinstance(x, float) and (pd.isna(x) or math.isnan(x)): return ""
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
    out = df.copy(); out.columns = cols
    return out

def _collapse_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = list(df.columns); seen = {}
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
    for c in ["src_table","src_column","tgt_table","tgt_column","tgt_datatype",
              "business_rule","join_clause","transformation_rule"]:
        if c not in df.columns: df[c] = ""
        df[c] = df[c].astype(str)
    df["src_table"] = df["src_table"].apply(lambda v: str(v).strip().split()[0].lower() if str(v).strip() else "")
    for c in ["src_column","tgt_column","tgt_datatype","business_rule","join_clause","transformation_rule"]:
        df[c] = df[c].apply(lambda v: s(v).lower())
    return df

def learn_source_columns(df: pd.DataFrame):
    out = {}
    for _, row in df.iterrows():
        t = s(row.get("src_table","")).lower(); c = s(row.get("src_column","")).lower()
        if not t or not c: continue
        if re.match(r"(?i)^t_[a-z0-9_]+_\d+$", c): continue
        if c == "nan": continue
        out.setdefault(t, set()).add(c)
    return out

def infer_aliases_globally(df: pd.DataFrame, nlp_alias: Dict[str,str]) -> Dict[str,str]:
    table_alias = {}
    for tbl, a in (nlp_alias or {}).items():
        if a and a.lower() not in GENERIC_BAD_ALIASES:
            table_alias[tbl.lower()] = a.lower()
    for _, row in df.iterrows():
        for col in ["join_clause","transformation_rule","business_rule"]:
            txt = s(row.get(col,""))
            if not txt: continue
            tnorm = re.sub(r"\s+", " ", txt)
            for m in re.finditer(r"(?i)\bfrom\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\b", tnorm):
                tbl, alias = m.group(1), m.group(2)
                tleaf, al = tbl.split(".")[-1].lower(), alias.lower()
                if al in GENERIC_BAD_ALIASES: continue
                table_alias.setdefault(tleaf, al)
            for m in re.finditer(r"(?i)\bjoin\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\b", tnorm):
                tbl, alias = m.group(1), m.group(2)
                tleaf, al = tbl.split(".")[-1].lower(), alias.lower()
                if al in GENERIC_BAD_ALIASES: continue
                table_alias.setdefault(tleaf, al)
    for t in sorted(set(df["src_table"])):
        if t and t not in table_alias:
            table_alias[t] = (t[:4] if t else "src").lower()
    return table_alias

def normalize_alias_usage(text: str, alias_map: Dict[str,str]) -> str:
    t = s(text)
    if not t: return t
    generic_to_table = {}
    for m in re.finditer(r"(?i)\bjoin\s+([A-Za-z0-9_\.]+)\s+(ref\d*|ref)\b", t):
        tbl = m.group(1).split(".")[-1].lower()
        generic_to_table[m.group(2).lower()] = tbl
    for m in re.finditer(r"(?i)\bfrom\s+([A-Za-z0-9_\.]+)\s+(ref\d*|ref)\b", t):
        tbl = m.group(1).split(".")[-1].lower()
        generic_to_table[m.group(2).lower()] = tbl
    for generic, tbl in generic_to_table.items():
        if tbl in alias_map:
            t = re.sub(rf"\b{re.escape(generic)}\.", f"{alias_map[tbl]}.", t)
    return t

def is_date_like(datatype: str) -> bool:
    d = (datatype or "").lower()
    return any(k in d for k in ["date", "timestamp", "datetime"])

def normalize_static_value(val: str) -> str:
    v = s(val).lower()
    if not v: return v
    m = re.match(r"(?i)^set\s+to\s+(.+)$", v)
    if m: v = m.group(1).strip()
    if v in {"set a to a --1a", "a", "'a'"}: return "'A'"
    v = re.sub(r"\s*\(.*?\)\.?$", "", v).strip().rstrip(".")
    if re.match(r"^\+?\d[\d]*$", v):
        neg = v.startswith("-")
        digits = re.sub(r"[^\d]", "", v).lstrip("0") or "0"
        return ("-" if neg else "") + digits
    if v in {"null"}: return "null"
    if v in {"current_timestamp", "current_timestamp()"}: return "current_timestamp()"
    return v

def strip_from_join(expr: str) -> str:
    e = re.split(r"(?i)\s+\bfrom\b", expr)[0]
    e = re.split(r"(?i)\s+(left|inner|right|full)\s+join\b", e)[0]
    return e.strip()

def harvest_case_blocks(text: str) -> List[str]:
    t = s(text)
    if not t: return []
    core = strip_from_join(t)
    segs = re.findall(r"(?is)(case .*? end)", core, flags=re.I)
    if segs: return [re.sub(r"\s+", " ", seg).strip().lower() for seg in segs]
    if "case" in core.lower():
        return [re.sub(r"\s+", " ", core).strip().lower()]
    return []

def business_rules_to_sql(raw: str, alias_map: Dict[str,str], this_alias: str) -> List[str]:
    out = []
    t = normalize_alias_usage(raw, alias_map).lower()
    bits = re.split(r"(?i)[;\n]| and |\d\)", t)
    for b in bits:
        bb = s(b).lower()
        if not bb: continue
        m = re.search(r"duplicate\s+([a-z][a-z0-9_]*)\.([a-z][a-z0-9_]*)", bb)
        if m:
            a, col = m.group(1), m.group(2)
            out.append(f"qualify row_number() over (partition by {a}.{col} order by {a}.{col}) = 1"); continue
        m = re.search(r"([a-z][a-z0-9_]*)\.([a-z][a-z0-9_]*)\s+is\s+all\s+spaces", bb)
        if m:
            a, col = m.group(1), m.group(2)
            out.append(f"nullif(regexp_replace(trim({a}.{col}), '\\s+', ''), '') is not null"); continue
        if "srstatus" in bb and "<>" in bb and "'a'" in bb:
            out.append(f"{this_alias}.srstatus = 'a'"); continue
        if "exclude inactive" in bb or "not active" in bb:
            out.append(f"{this_alias}.srstatus = 'a'"); continue
        if "mutual fund instrument price file" in bb or "mfspric" in bb:
            glsx_a = alias_map.get("glsxref", "glsx")
            mfsp_a = alias_map.get("mfspric", "mfsp")
            out.append(f"not exists (select 1 from {mfsp_a} where substring({glsx_a}.send_cd, 4, 5) = {mfsp_a}.prc_dtl_send_num)"); continue
    clean, seen = [], set()
    for w in out:
        if w not in seen:
            seen.add(w); clean.append(w)
    return clean

def build_summary(csv_path: str, nlp_json_path: str, outdir: str):
    df = load_csv(csv_path)
    try:
        nlp = json.loads(Path(nlp_json_path).read_text())
    except Exception:
        nlp = {}
    nlp_alias = {k.lower(): (v.get("alias","") if isinstance(v, dict) else "") for k, v in (nlp or {}).items()}
    alias_map = infer_aliases_globally(df, nlp_alias)
    known_cols_by_src = learn_source_columns(df)

    summary = {}
    for src in sorted(set(df["src_table"])):
        if not src: continue
        alias = alias_map.get(src, (src[:4] if src else "src")).lower()
        sdf = df[df["src_table"] == src]
        joins = [normalize_alias_usage(s(r.get("join_clause","")), alias_map) for _, r in sdf.iterrows() if s(r.get("join_clause",""))]
        brs   = [normalize_alias_usage(s(r.get("business_rule","")), alias_map) for _, r in sdf.iterrows() if s(r.get("business_rule",""))]
        trs   = [normalize_alias_usage(s(r.get("transformation_rule","")), alias_map) for _, r in sdf.iterrows() if s(r.get("transformation_rule",""))]

        # JOIN normalization (v7c)
        join_logic = []
        for j in joins:
            jj = re.sub(r"\s+", " ", j).strip().lower()

            # if it's "with" syntax (mas with glsxref ...) → "left join glsxref ..."
            if re.search(r"\bwith\b", jj) and " on " in jj and " join " not in jj:
                jj = re.sub(r"^[a-z0-9_]+\s+with\s+", "left join ", jj)

            # if it starts directly with "join", upgrade to "left join" unless explicitly says inner join
            if re.match(r"^\s*join\b", jj):
                if " inner join " in f" {jj} ":
                    jj = re.sub(r"^\s*join\b", "inner join", jj)
                else:
                    jj = re.sub(r"^\s*join\b", "left join", jj)

            # ensure no stray 'mas with' or duplicate whitespace
            jj = re.sub(r"\b[a-z0-9_]+\s+with\s+", "", jj)
            jj = re.sub(r"\s+", " ", jj).strip()

            if jj and jj not in join_logic:
                join_logic.append(jj)

        # remove duplicates (order preserved)
        join_logic = list(dict.fromkeys(join_logic))


        # Business rules → SQL
        br_sql = []
        for b in brs:
            br_sql.extend(business_rules_to_sql(b, alias_map, alias))
        br_sql = list(dict.fromkeys(br_sql))

        candidate_where = list(dict.fromkeys(br_sql))

        # Derived
        derived = []
        seen_der = set()
        for _, r in sdf.iterrows():
            cases = harvest_case_blocks(s(r.get("transformation_rule","")))
            if not cases: continue
            tgt = s(r.get("tgt_column","")).lower() or "derived_case"
            for c in cases:
                key = (tgt, c)
                if key in seen_der: continue
                seen_der.add(key)
                derived.append({"name": tgt, "expression": c})

        # Referenced + lineage
        known_cols = set([c.lower() for c in known_cols_by_src.get(src, set())])
        referenced, seen_ref = [], set()
        lineage, seen_lin = [], set()
        texts = join_logic + brs + trs
        for t in texts:
            for (qual, col) in QUAL_ID_RX.findall(t):
                ql = f"{qual.lower()}.{col.lower()}"
                if ql not in seen_lin:
                    seen_lin.add(ql); lineage.append(ql)
                if col.lower() not in seen_ref:
                    seen_ref.add(col.lower()); referenced.append(col.lower())
            for tok in re.findall(r"[A-Za-z][A-Za-z0-9_]*", t):
                lo = tok.lower()
                if lo in known_cols and lo not in seen_ref:
                    seen_ref.add(lo); referenced.append(lo)
        # clean lineage and referenced_columns noise
        lineage = [x for x in lineage if x not in {"i.e", "e"}]
        referenced = [x for x in referenced if x not in {"i.e", "e"}]

        # Static assignments
        static_assignments = []
        for _, r in sdf.iterrows():
            tgt_col = s(r.get("tgt_column","")).lower()
            if not tgt_col: continue
            tr = s(r.get("transformation_rule","")).lower()
            if not tr: continue
            if "straight move" in tr:
                src_col = s(r.get("src_column","")).lower()
                if src_col:
                    expr = f"{alias}.{src_col}"
                    if is_date_like(s(r.get('tgt_datatype','')).lower()):
                        expr = f"to_date({expr}, 'yyyy-MM-dd')"
                    static_assignments.append({"value": expr, "target_column": tgt_col})
                continue
            if tgt_col == "last_change_dt" and "etl.effective.start.date" in tr:
                static_assignments.append({
                    "value": "to_date('\"\"\"${etl.effective.start.date}\"\"\"', 'yyyyMMddHHmmss')",
                    "target_column": tgt_col
                }); continue
            if tgt_col == "to_dt" and ("9999-12-31" in tr or "cast it as date" in tr):
                static_assignments.append({
                    "value": "to_date('9999-12-31', 'yyyy-MM-dd')",
                    "target_column": tgt_col
                }); continue
            if re.search(r"(?i)\bset\s+to\b", tr) or tr in {"null","'a'","a"} or "current_timestamp" in tr or "to_date(" in tr or re.match(r"^[\+\-]?\d", tr):
                v = normalize_static_value(tr)
                static_assignments.append({"value": v, "target_column": tgt_col})

        summary[src] = {
            "alias": alias,
            "known_columns": sorted(list(known_cols)),
            "referenced_columns": referenced,
            "candidate_where_predicates": candidate_where,
            "join_logic": join_logic,
            "business_rules": br_sql,
            "lineage": {"source_level": lineage},
            "derived_columns": derived,
            "static_assignments": static_assignments,
        }

    out = Path(outdir); out.mkdir(parents=True, exist_ok=True)
    (out / "source_columns_summary_v7c.json").write_text(json.dumps(summary, indent=2))

    md_lines = ["# source columns summary v7c\n"]
    for src, d in summary.items():
        md_lines.append(f"## {src}")
        md_lines.append(f"- alias: `{d['alias']}`")
        if d["join_logic"]:
            md_lines.append("- join_logic:")
            for j in d["join_logic"]: md_lines.append(f"  - `{j}`")
        if d["candidate_where_predicates"]:
            md_lines.append("- where_like:")
            for w in d["candidate_where_predicates"]: md_lines.append(f"  - `{w}`")
        if d["static_assignments"]:
            md_lines.append("- static_assignments:")
            for srow in d["static_assignments"][:5]:
                md_lines.append(f"  - {srow['target_column']} ← {srow['value']}")
        md_lines.append("")
    (out / "source_columns_summary_v7c.md").write_text("\n".join(md_lines))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("csv")
    ap.add_argument("--nlp_json", required=True)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()
    build_summary(args.csv, args.nlp_json, args.outdir)
