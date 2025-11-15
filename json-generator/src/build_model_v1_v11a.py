import json, re, argparse, os
from collections import defaultdict

def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def uniq(seq):
    seen = set()
    out = []
    for x in seq:
        key = json.dumps(x, sort_keys=True) if isinstance(x, dict) else str(x).lower()
        if key not in seen:
            seen.add(key)
            out.append(x)
    return out

def scan_expr_tables(expr):
    """Extract potential table/alias names from expressions like ref.column or glsx.send_cd"""
    return re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\.", expr)

def infer_datatype(expr):
    e = (expr or "").lower()
    if "to_date(" in e:
        return "DATE"
    if "current_timestamp" in e:
        return "TIMESTAMP"
    if "try_cast" in e:
        m = re.search(r"try_cast\(.* as ([a-z0-9_()]+)\)", e)
        if m:
            return m.group(1).upper()
    if re.search(r"\d+\.\d+", e):
        return "DECIMAL(17,2)"
    if re.search(r"[+-]?\d+\b", e):
        return "BIGINT"
    return "STRING"

def build_cast_expression(value, dtype):
    v = str(value).strip()
    if re.match(r"to_date|current_timestamp", v, re.I):
        return f"CAST({v} AS {dtype})"
    if dtype.startswith("DECIMAL"):
        return f"COALESCE(TRY_CAST({v} AS {dtype}), TRY_CAST(NULL AS {dtype}))"
    if dtype in ("BIGINT","INT","DOUBLE"):
        return f"TRY_CAST({v} AS {dtype})"
    return f"CAST({v} AS {dtype})"

def choose_base_entity(v7):
    scored = [(len(meta.get("derived_columns", [])), ent) for ent, meta in v7.items()]
    scored.sort(reverse=True)
    return scored[0][1] if scored else list(v7.keys())[0]

def infer_joins(v7, base_entity, debug=False):
    joins = []
    # collect explicit joins
    for ent, meta in v7.items():
        for j in meta.get("join_logic", []) or []:
            joins.append(j.strip())

    # map aliases from JSON
    alias_to_entity = {}
    for ent, meta in v7.items():
        alias = (meta.get("alias") or "").lower()
        if alias:
            alias_to_entity[alias] = ent.lower()

    derived_refs = defaultdict(set)
    for ent, meta in v7.items():
        for d in meta.get("derived_columns", []) or []:
            expr = d.get("expression", "")
            refs = scan_expr_tables(expr)
            for r in refs:
                r_lower = r.lower()
                if r_lower in alias_to_entity:
                    derived_refs[ent].add(alias_to_entity[r_lower])
                elif r_lower in v7:
                    derived_refs[ent].add(r_lower)

    for ref in derived_refs.get(base_entity, []):
        if ref != base_entity:
            joins.append(f"LEFT JOIN {ref} ON {base_entity}.srseccode = {ref}.srseccode")

    joins = uniq(joins)
    if debug:
        print("---- inferred joins ----")
        for j in joins: print(j)
    return joins

def build_model(v7, debug=False):
    base = choose_base_entity(v7)
    joins = infer_joins(v7, base, debug)
    # business rules
    rules = []
    for meta in v7.values():
        rules += meta.get("business_rules", []) or []
        rules += meta.get("candidate_where_predicates", []) or []
    rules = uniq(rules)

    # derived
    derived = []
    for meta in v7.values():
        for d in meta.get("derived_columns", []) or []:
            expr = d.get("expression", "")
            derived.append({
                "target": d.get("name"),
                "expression": expr,
                "datatype": infer_datatype(expr)
            })

    # statics
    statics = []
    for meta in v7.values():
        for s in meta.get("static_assignments", []) or []:
            val = s.get("value", "")
            tgt = s.get("target_column")
            dtype = infer_datatype(val)
            expr = build_cast_expression(val, dtype)
            statics.append({
                "target": tgt,
                "expression": expr,
                "datatype": dtype
            })

    model = {
        "base_entity": base,
        "joins": joins,
        "business_rules": rules,
        "derived": uniq(derived),
        "statics": uniq(statics)
    }
    return model

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--debug-joins", action="store_true")
    args = ap.parse_args()
    v7 = read_json(args.input)
    model = build_model(v7, args.debug_joins)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(model, f, indent=2)
    print(f"✅ Model written to {args.output}")
    print(f"Base entity: {model['base_entity']}")
    print(f"Joins: {len(model['joins'])}, Derived: {len(model['derived'])}, Statics: {len(model['statics'])}")

if __name__ == "__main__":
    main()
