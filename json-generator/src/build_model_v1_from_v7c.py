import json, re
from pathlib import Path

def normalize_type(dtype: str) -> str:
    """Normalize datatype for consistent comparison."""
    if not dtype:
        return "STRING"
    d = dtype.strip().lower()
    if "decimal" in d:
        return "DECIMAL"
    if d in ("int", "integer", "bigint", "int64", "int32"):
        return "BIGINT"
    if d in ("date", "timestamp", "datetime"):
        return d.upper()
    return "STRING"

def build_cast(expr: str, src_dt: str, tgt_dt: str) -> str:
    """Apply smart casting only when needed."""
    src = normalize_type(src_dt)
    tgt = normalize_type(tgt_dt)
    if src == tgt:
        return expr
    if tgt == "DECIMAL":
        return f"COALESCE(TRY_CAST({expr} AS DECIMAL(17,2)), 0.0)"
    if tgt in ("BIGINT", "INT"):
        return f"CAST({expr} AS {tgt})"
    if tgt in ("DATE", "TIMESTAMP"):
        return f"CAST({expr} AS {tgt})"
    return expr

def build_model_v1(v7c_path: str, out_path: str):
    data = json.loads(Path(v7c_path).read_text(encoding="utf-8"))

    # pick base table (one with most derived columns)
    base = max(data.items(), key=lambda kv: len(kv[1].get("derived_columns", [])))[0]
    base_alias = data[base].get("alias", base[:3])

    joins, rules, columns = set(), set(), {}

    for ent, meta in data.items():
        # collect joins
        for j in meta.get("join_logic", []) or []:
            joins.add(j.strip())

        # collect business rules
        for r in meta.get("business_rules", []) or []:
            if r.strip():
                rules.add(r.strip())

        # derived columns
        for d in meta.get("derived_columns", []) or []:
            tgt = d.get("name", "").strip().lower()
            expr = d.get("expression", "").strip()
            if tgt and expr:
                columns[tgt] = {
                    "target": tgt,
                    "expression": expr,
                    "datatype": "STRING"
                }

        # static assignments
        for s in meta.get("static_assignments", []) or []:
            tgt = s.get("target_column", "").strip().lower()
            val = s.get("value", "").strip()
            if not tgt or not val:
                continue
            # guess datatype from value
            if re.fullmatch(r"[-+]?\d+", val):
                dt = "BIGINT"
            elif "date" in val.lower():
                dt = "DATE"
            else:
                dt = "STRING"
            expr = val
            if dt == "BIGINT":
                expr = f"CAST({val} AS BIGINT)"
            elif dt == "DATE" and "to_date" not in val.lower():
                expr = f"CAST({val} AS DATE)"
            columns[tgt] = {
                "target": tgt,
                "expression": expr,
                "datatype": dt
            }

    model = {
        "base": {"entity": base, "alias": base_alias},
        "joins": sorted(list(joins)),
        "columns": list(columns.values()),
        "business_rules": sorted(list(rules))
    }

    Path(out_path).write_text(json.dumps(model, indent=2), encoding="utf-8")
    print(f"✅ model_v1.json created at: {out_path}")

if __name__ == "__main__":
    v7c_path = "json-generator/generated_out/source_columns_summary_v7c.json"
    out_path = "json-generator/generated_out/model_v1.json"
    build_model_v1(v7c_path, out_path)
