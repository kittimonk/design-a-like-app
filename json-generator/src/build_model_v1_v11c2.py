
import json, re, argparse, os
from collections import defaultdict

EQ_RE = re.compile(r"\b([A-Za-z_]\w*)\.(\w+)\s*=\s*([A-Za-z_]\w*)\.(\w+)")
SUBSTR_EQ_RE = re.compile(
    r"substring\(\s*([A-Za-z_]\w*)\.(\w+)\s*,\s*\d+\s*,\s*\d+\s*\)\s*=\s*([A-Za-z_]\w*)\.(\w+)",
    re.IGNORECASE
)

def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def uniq(seq):
    seen = set(); out = []
    for x in seq:
        k = json.dumps(x, sort_keys=True) if isinstance(x, dict) else str(x).lower()
        if k not in seen:
            seen.add(k); out.append(x)
    return out

def scan_expr_tables(expr):
    return re.findall(r"\b([A-Za-z_]\w*)\.", expr or "")

def infer_datatype(expr_or_val):
    e = (str(expr_or_val) or "").lower()
    if "to_date(" in e: return "DATE"
    if "current_timestamp" in e: return "TIMESTAMP"
    if "decimal" in e or re.search(r"\d+\.\d+", e): return "DECIMAL(17,2)"
    m = re.search(r"try_cast\([^)]* as ([a-z0-9_()]+)\)", e)
    if m: return m.group(1).upper()
    # detect integers even in quotes or with leading +
    if re.fullmatch(r"\s*'?[\+\-]?\d+'\s*", e) or re.search(r"[+\-]?\b\d+\b", e):
        return "BIGINT"
    return "STRING"

def build_cast_expression(value, dtype):
    v = str(value).strip()
    if re.match(r"to_date|current_timestamp", v, re.I):
        return f"CAST({v} AS {dtype})"
    if dtype.startswith("DECIMAL"):
        return f"COALESCE(TRY_CAST({v} AS {dtype}), TRY_CAST(NULL AS {dtype}))"
    if dtype in ("BIGINT","INT","DOUBLE","FLOAT"):
        return f"TRY_CAST({v} AS {dtype})"
    return f"CAST({v} AS {dtype})"

def choose_base_entity(v7):
    # Prefer entity that appears most across derived expressions (not hardcoded)
    counts = defaultdict(int)
    for ent, meta in v7.items():
        for d in meta.get("derived_columns") or []:
            expr = d.get("expression", "") or ""
            for t in scan_expr_tables(expr):
                counts[t.lower()] += 1
    if counts:
        # pick the entity whose key (entity or alias) appears most
        best = max(counts.items(), key=lambda x: x[1])[0]
        # find matching entity name if best is an alias key in v7
        if best in [e.lower() for e in v7.keys()]:
            return [e for e in v7.keys() if e.lower()==best][0]
    # fallback to the entity with most derived columns
    scored = [(len(m.get("derived_columns", [])), ent) for ent, m in v7.items()]
    scored.sort(reverse=True)
    return scored[0][1] if scored else list(v7.keys())[0]

def alias_map(v7):
    a2e, e2a = {}, {}
    for ent, meta in v7.items():
        al = (meta.get("alias") or "").strip()
        if al:
            a2e[al.lower()] = ent
            e2a[ent.lower()] = al
    return a2e, e2a

def normalize_side(name, a2e):
    n = name.lower()
    if n in a2e:
        ent = a2e[n]
        return ent, n
    # also if name matches an entity key
    return name, None

def render_join(left_ent, left_alias, left_col, right_ent, right_alias, right_col):
    L = f"{left_alias}" if left_alias else f"{left_ent}"
    R = f"{right_alias}" if right_alias else f"{right_ent}"
    return f"LEFT JOIN {right_ent}{(' ' + right_alias) if right_alias else ''} ON {L}.{left_col} = {R}.{right_col}"

def extract_explicit_joins(v7):
    joins = []
    for _, meta in v7.items():
        for j in (meta.get("join_logic") or []):
            j = re.sub(r"\s+", " ", (j or "").strip())
            if j:
                joins.append(j)
    return uniq(joins)

def mine_equality_predicates(text):
    preds = []
    for m in EQ_RE.finditer(text):
        preds.append(("EQ", m.group(1), m.group(2), m.group(3), m.group(4)))
    for m in SUBSTR_EQ_RE.finditer(text):
        preds.append(("SUBSTR_EQ", m.group(1), m.group(2), m.group(3), m.group(4)))
    return preds

def infer_joins(v7, base_entity, debug=False):
    a2e, e2a = alias_map(v7)

    # 1) explicit
    joins = extract_explicit_joins(v7)

    # 2) mine predicates from all expressions/rules
    hay = []
    for _, meta in v7.items():
        hay += (meta.get("business_rules") or [])
        hay += (meta.get("candidate_where_predicates") or [])
        for d in (meta.get("derived_columns") or []):
            if d.get("expression"):
                hay.append(d["expression"])

    mined = []
    for txt in hay:
        mined += mine_equality_predicates(txt or "")

    auto_joins = []
    for kind, a1, c1, a2, c2 in mined:
        left_ent, left_alias = normalize_side(a1, a2e)
        right_ent, right_alias = normalize_side(a2, a2e)

        # prefer base on the left
        if left_ent.lower() != base_entity.lower() and right_ent.lower() == base_entity.lower():
            left_ent, right_ent = right_ent, left_ent
            left_alias, right_alias = right_alias, left_alias
            c1, c2 = c2, c1

        if kind == "EQ":
            auto_joins.append(render_join(left_ent, left_alias, c1, right_ent, right_alias, c2))
        elif kind == "SUBSTR_EQ":
            L = f"{left_alias}" if left_alias else f"{left_ent}"
            R = f"{right_alias}" if right_alias else f"{right_ent}"
            auto_joins.append(f"LEFT JOIN {right_ent}{(' ' + right_alias) if right_alias else ''} "
                              f"ON substring({L}.{c1}, 4, 5) = {R}.{c2}")

    # keep only meaningful LEFT JOIN... ON ... (block weird fragments)
    auto_joins = [j for j in auto_joins if j.lower().startswith("left join") and " on " in j.lower()]
    joins = uniq(joins + auto_joins)

    if debug:
        print("---- inferred joins ----")
        for j in joins:
            print(j)
    return joins

def pick_target_name(d):
    # try common keys in order
    for k in ("target", "name", "target_column", "column", "Column/Field Name *"):
        if k in d and d[k]:
            return d[k]
    return ""

def build_model(v7, debug=False):
    base = choose_base_entity(v7)
    joins = infer_joins(v7, base, debug)

    # Business rules
    rules = []
    for meta in v7.values():
        rules += meta.get("business_rules", []) or []
        rules += meta.get("candidate_where_predicates", []) or []
    rules = uniq([re.sub(r"\s+", " ", (r or "").strip()) for r in rules if (r or "").strip()])

    # Derived
    seen_derived = set(); derived = []
    for meta in v7.values():
        for d in meta.get("derived_columns") or []:
            tgt = pick_target_name(d)
            expr = (d.get("expression") or "").strip()
            if not expr:
                continue
            key = (tgt or "").lower() + "|" + expr.lower()
            if key in seen_derived:
                continue
            seen_derived.add(key)
            dtype = d.get("datatype") or infer_datatype(expr)
            derived.append({
                "target": tgt,
                "expression": expr,
                "datatype": dtype
            })

    # Statics
    seen_static = set(); statics = []
    for meta in v7.values():
        for s in meta.get("static_assignments") or []:
            tgt = s.get("target_column") or s.get("target") or ""
            val = s.get("value", "")
            if tgt == "" and val == "": 
                continue
            key = (tgt or "").lower() + "|" + str(val).strip().lower()
            if key in seen_static:
                continue
            seen_static.add(key)
            dtype = s.get("datatype") or infer_datatype(val)
            expr = build_cast_expression(val, dtype)
            statics.append({
                "target": tgt,
                "expression": expr,
                "datatype": dtype
            })

    return {
        "base_entity": base,
        "joins": joins,
        "business_rules": rules,
        "derived": derived,
        "statics": statics
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="path to source_columns_summary_v7c.json")
    ap.add_argument("--output", required=True, help="path to write model_v1.json")
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
