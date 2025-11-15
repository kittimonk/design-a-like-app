import json, re, argparse, os
from collections import defaultdict

EQ_RE = re.compile(r"\b([A-Za-z_]\w*)\.(\w+)\s*=\s*([A-Za-z_]\w*)\.(\w+)")
SUBSTR_EQ_RE = re.compile(
    r"substring\(\s*([A-Za-z_]\w*)\.(\w+)\s*,\s*\d+\s*,\s*\d+\s*\)\s*=\s*([A-Za-z_]\w*)\.(\w+)",
    re.IGNORECASE
)

# ---------- Utility functions ----------
def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def uniq(seq):
    seen = set(); out = []
    for x in seq:
        k = json.dumps(x, sort_keys=True) if isinstance(x, dict) else str(x).lower()
        if k not in seen:
            seen.add(k)
            out.append(x)
    return out

def scan_expr_tables(expr):
    return re.findall(r"\b([A-Za-z_]\w*)\.", expr or "")

def infer_datatype(expr):
    e = (expr or "").lower()
    if re.search(r"'[yn]'", e) or ("'y'" in e or "'n'" in e):
        return "STRING"
    if "to_date(" in e: return "DATE"
    if "current_timestamp" in e: return "TIMESTAMP"
    if "float" in e or "decimal" in e: return "DECIMAL(17,2)"
    m = re.search(r"try_cast\([^)]* as ([a-z0-9_()]+)\)", e)
    if m: return m.group(1).upper()
    if re.search(r"\d+\.\d+", e): return "DECIMAL(17,2)"
    if re.search(r"[+-]?\d+\b", e): return "BIGINT"
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
    return name, None

def render_join(left_ent, left_alias, left_col, right_ent, right_alias, right_col):
    L = f"{left_alias}" if left_alias else f"{left_ent}"
    R = f"{right_alias}" if right_alias else f"{right_ent}"
    return f"LEFT JOIN {right_ent}{(' ' + right_alias) if right_alias else ''} ON {L}.{left_col} = {R}.{right_col}"

def extract_explicit_joins(v7):
    joins = []
    for _, meta in v7.items():
        for j in meta.get("join_logic", []) or []:
            j = re.sub(r"\s+", " ", j.strip())
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

# ---------- Join inference ----------
def infer_joins(v7, base_entity, debug=False):
    a2e, e2a = alias_map(v7)
    joins = extract_explicit_joins(v7)

    # mine from expressions and rules
    haystacks = []
    for _, meta in v7.items():
        haystacks += (meta.get("business_rules") or [])
        haystacks += (meta.get("candidate_where_predicates") or [])
        for d in meta.get("derived_columns") or []:
            if d.get("expression"):
                haystacks.append(d["expression"])

    mined = []
    for txt in haystacks:
        mined += mine_equality_predicates(txt)

    auto_joins = []
    for kind, a1, c1, a2, c2 in mined:
        left_ent, left_alias = normalize_side(a1, a2e)
        right_ent, right_alias = normalize_side(a2, a2e)

        # Prefer base_entity on left
        if left_ent.lower() != base_entity.lower() and right_ent.lower() == base_entity.lower():
            left_ent, right_ent = right_ent, left_ent
            left_alias, right_alias = right_alias, left_alias
            c1, c2 = c2, c1

        if kind == "EQ":
            auto_joins.append(render_join(left_ent, left_alias, c1, right_ent, right_alias, c2))
        elif kind == "SUBSTR_EQ":
            L = f"{left_alias}" if left_alias else f"{left_ent}"
            R = f"{right_alias}" if right_alias else f"{right_ent}"
            auto_joins.append(
                f"LEFT JOIN {right_ent}{(' ' + right_alias) if right_alias else ''} "
                f"ON substring({L}.{c1}, 4, 5) = {R}.{c2}"
            )

    all_joins = uniq(joins + auto_joins)

    # remove self joins & duplicates (by target entity)
    clean = []
    seen_entities = set()
    for j in all_joins:
        # skip self join
        if re.search(fr"\b{base_entity}\b", j, re.I) and \
           len(re.findall(fr"\b{base_entity}\b", j, re.I)) > 1:
            continue
        # dedupe target table
        m = re.search(r"join\s+([a-zA-Z_][a-zA-Z0-9_]*)", j, re.I)
        if not m: 
            clean.append(j); continue
        ent = m.group(1).lower()
        if ent not in seen_entities:
            seen_entities.add(ent)
            clean.append(j)
    if debug:
        print("---- inferred joins ----")
        for j in clean:
            print(j)
    return clean

# ---------- Model builder ----------
def build_model(v7, debug=False):
    base = choose_base_entity(v7)
    joins = infer_joins(v7, base, debug)

    rules = []
    for meta in v7.values():
        rules += meta.get("business_rules", []) or []
        rules += meta.get("candidate_where_predicates", []) or []
    rules = uniq(rules)

    # derived
    seen_derived = set(); derived = []
    for meta in v7.values():
        for d in meta.get("derived_columns", []) or []:
            tgt = d.get("name")
            expr = d.get("expression", "")
            key = (tgt or "").lower() + "|" + re.sub(r"\s+", " ", expr.strip().lower())
            if key not in seen_derived:
                seen_derived.add(key)
                derived.append({
                    "target": tgt,
                    "expression": expr,
                    "datatype": infer_datatype(expr)
                })

    # statics
    seen_static = set(); statics = []
    for meta in v7.values():
        for s in meta.get("static_assignments", []) or []:
            tgt = s.get("target_column")
            val = s.get("value", "")
            key = (tgt or "").lower() + "|" + str(val).strip().lower()
            if key not in seen_static:
                seen_static.add(key)
                dtype = infer_datatype(val)
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

# ---------- CLI ----------
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
