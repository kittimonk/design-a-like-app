
#!/usr/bin/env python3
import argparse, json, re
from pathlib import Path
import pandas as pd
from typing import List, Dict, Set, Tuple
from common_utils import load_csv, s, learn_source_columns, find_alias_for_source, harvest_identifiers_for_source, normalize_join_text, unique_joins, strip_from_join

def build_per_source_sql(csv_path: str, outdir: str, target: str, malcode: str) -> None:
    df = load_csv(csv_path)
    src_cols_map = learn_source_columns(df)
    sources = [s for s in df["src_table"].astype(str).str.strip().unique().tolist() if s]

    modules = {}
    logs = []

    # Helper to build one source
    def build_sql_for_source(source: str) -> str:
        sdf = df[df["src_table"].astype(str).str.strip().str.lower()==source.lower()].copy()

        # texts & alias
        texts=[]
        for col in ["join_clause","business_rule","transformation_rule"]:
            if col in sdf.columns:
                ser = sdf[col] if not isinstance(sdf[col], pd.DataFrame) else sdf[col].iloc[:,0]
                texts.extend([s(v) for v in ser if s(v)])
        alias = find_alias_for_source(source, texts) or ("mas" if source.lower()=="ossbr_2_1" else source[:4].lower())
        known_cols = src_cols_map.get(source.lower(), set())

        # columns to include
        referenced = harvest_identifiers_for_source(source, texts, known_cols, alias)
        direct_series = sdf["src_column"] if "src_column" in sdf.columns else pd.Series([], dtype=str)
        direct_cols=set()
        for v in direct_series:
            vn = s(v).strip().replace("-","_")
            if vn and vn.lower()!="nan" and not re.match(r"(?i)^t_[a-z0-9_]+_\d+$", vn):
                direct_cols.add(vn)
        cols = []
        for c in referenced:
            if c not in cols:
                cols.append(c)
        for c in direct_cols:
            if c not in cols:
                cols.append(c)
        if not cols:
            cols = sorted(list(known_cols))
        select_cols = ",\n    ".join(f"{alias}.{c}" for c in cols) if cols else f"{alias}.*"
        sql = f"SELECT\n    {select_cols}\nFROM {source} {alias}"

        # joins: include only if clearly attached to this source
        joins=[]
        if "join_clause" in sdf.columns:
            for v in sdf["join_clause"]:
                jtxt = s(v)
                if not jtxt: continue
                # custom grammar JOIN A a WITH B b ON ...
                m = re.search(r"(?is)\bjoin\s+([A-Za-z0-9_\.]+)\s+([A-Za-z][A-Za-z0-9_]*)\s+with", jtxt)
                if m:
                    left_tbl = m.group(1).split(".")[-1].lower()
                    if left_tbl == source.lower():
                        joins.append(normalize_join_text(jtxt))
                else:
                    # std joins kept only if ON clause references our alias
                    if re.search(rf"\b{alias}\.", jtxt):
                        joins.append(normalize_join_text(jtxt))
        # automatic inferred joins
        if source.lower()=="glsxref":
            if any(re.search(r"substring\(\s*"+re.escape(alias)+r"\.send_cd", t, flags=re.I) for t in texts):
                joins.append(f"LEFT JOIN dt_{target}_{malcode.lower()}_MFIN mfn ON SUBSTRING({alias}.SEND_CD, 4, 5) = mfn.MFIN_SEND_NUMBER")
                joins.append(f"LEFT JOIN dt_{target}_{malcode.lower()}_MFSPRIC mfsp ON SUBSTRING({alias}.SEND_CD, 4, 5) = mfsp.PRC_DTL_SEND_NUM")
        if source.lower()=="ossbr_2_1":
            joins.append(f"LEFT JOIN dt_{target}_{malcode.lower()}_GLSXREF glsx ON {alias}.SRSECCODE = glsx.WASTE_SECURITY_CODE")
            joins.append(f"LEFT JOIN dt_{target}_{malcode.lower()}_GLSXREF glsx2 ON {alias}.SRSECCODE = glsx2.sm_SECURITY_CODE")
            joins.append(f"LEFT JOIN dt_{target}_{malcode.lower()}_tantrum tant ON {alias}.SRSECCODE = tant.SRSECCODE")

        joins = unique_joins([j for j in joins if j], base_alias=alias)
        if joins:
            sql += "\n" + "\n".join(joins)

        # WHERE from business rules for local alias only
        where_terms=[]
        if "business_rule" in sdf.columns:
            for v in sdf["business_rule"]:
                txt = s(v)
                for line in re.split(r"\n| and ", txt, flags=re.I):
                    L=line.strip()
                    if not L: continue
                    if re.search(rf"\b{alias}\.[A-Za-z][A-Za-z0-9_]*\b", L): 
                        where_terms.append(L)
                    elif source.lower()=="ossbr_2_1" and re.search(r"(?i)\bSRSTATUS\s*<>\s*'A'\b", L):
                        where_terms.append(f"{alias}.SRSTATUS = 'A'")
        if source.lower()=="ossbr_2_1" and not any("SRSTATUS" in w for w in where_terms):
            where_terms.append(f"{alias}.SRSTATUS = 'A'")

        if where_terms:
            # dedup
            seen=set(); dedup=[]
            for w in where_terms:
                sig = re.sub(r"\s+"," ",w.lower())
                if sig in seen: continue
                seen.add(sig); dedup.append(w)
            sql += "\nWHERE\n  " + "\n  AND ".join(dedup)

        # log
        logs.append({
            "source": source,
            "alias": alias,
            "columns": cols,
            "joins": joins,
            "where_terms": where_terms,
        })
        return sql

    out = Path(outdir); out.mkdir(parents=True, exist_ok=True)
    for src in sources:
        view_name = f"dt_{target}_{malcode.lower()}_{src}"
        sql_text = build_sql_for_source(src)
        (out / f"{view_name}.sql").write_text(sql_text + "\n")
        modules[view_name] = {
            "sql": f'"""{sql_text}"""',
            "loggable": True,
            "options": {"module": "data_transformation", "method":"process"},
            "name": view_name
        }

    # write module json and log
    (out / "data_transformation_modules.json").write_text(json.dumps(modules, indent=2))

    # human log
    md_lines = ["# Data Transformation Modules — Build Log\n"]
    for item in logs:
        md_lines.append(f"## {item['source']} (alias `{item['alias']}`)")
        md_lines.append("- Columns: " + (", ".join(item["columns"]) or "(none)"))
        if item["joins"]:
            md_lines.append("- Joins:")
            for j in item["joins"]:
                md_lines.append(f"  - `{j}`")
        if item["where_terms"]:
            md_lines.append("- WHERE:")
            for w in item["where_terms"]:
                md_lines.append(f"  - `{w}`")
        md_lines.append("")
    (out / "data_transformation_modules.md").write_text("\n".join(md_lines))

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("csv")
    p.add_argument("--outdir", required=True)
    p.add_argument("--target", required=True)
    p.add_argument("--malcode", required=True)
    args = p.parse_args()
    build_per_source_sql(args.csv, args.outdir, args.target, args.malcode)
