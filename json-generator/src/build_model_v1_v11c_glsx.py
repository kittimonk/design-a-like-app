import json, re, argparse, os
from collections import defaultdict

# Simple equality: A.col = B.col
EQ_RE = re.compile(
    r"\b([A-Za-z_]\w*)\.(\w+)\s*=\s*([A-Za-z_]\w*)\.(\w+)"
)

# Substring equality: substring(A.col, n, m) = B.col
SUBSTR_EQ_RE = re.compile(
    r"substring\(\s*([A-Za-z_]\w*)\.(\w+)\s*,\s*\d+\s*,\s*\d+\s*\)\s*=\s*([A-Za-z_]\w*)\.(\w+)",
    re.IGNORECASE
)

def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def uniq(seq):
    seen = set()
    out = []
    for x in seq:
        k = json.dumps(x, sort_keys=True) if isinstance(x, dict) else re.sub(r"\s+", " ", str(x)).lower()
        if k not in seen:
            seen.add(k)
            out.append(x)
    return out

def scan_expr_tables(expr):
    return re.findall(r"\b([A-Za-z_]\w*)\.", expr or "")

def infer_datatype(expr):
    e = (expr or "").lower()
    if "to_date(" in e:
        return "DATE"
    if "current_timestamp" in e:
        return "TIMESTAMP"
    if "decimal" in e or re.search(r"\d+\.\d+", e):
        return "DECIMAL(17,2)"
    m = re.search(r"try_cast\([^)]* as ([a-z0-9_()]+)\)", e)
    if m:
        return m.group(1).upper()
    if re.search(r"[+-]?\d+\b", e):
        return "BIGINT"
    return "STRING"

def build_cast_expression(value, dtype):
    v = str(value).strip()
    if re.match(r"to_date|current_timestamp", v, re.I):
        return f"CAST({v} AS {dtype})"
    if dtype.startswith("DECIMAL"):
        return f"COALESCE(TRY_CAST({v} AS {dtype}), TRY_CAST(NULL AS {dtype}))"
    if dtype in ("BIGINT", "INT", "DOUBLE", "FLOAT"):
        return f"TRY_CAST({v} AS {dtype})"
    return f"CAST({v} AS {dtype})"

def choose_base_entity(v7):
    scored = [(len(m.get("derived_columns", [])), ent) for ent, m in v7.items()]
    scored.sort(reverse=True)
    return scored[0][1] if scored else list(v7.keys())[0]

def alias_map(v7):
    """
    Build:
      - a2e: alias -> entity
      - e2a: entity -> alias

    Special rule per your request:
      any entity whose name contains 'glsx' but has no alias
      gets canonical alias 'glsx'.
    """
    a2e = {}
    e2a = {}
    for ent, meta in v7.items():
        ent_l = ent.lower()
        al = (meta.get("alias") or "").strip()
        if not al and "glsx" in ent_l:
            # your requested canonical alias; this is the ONLY fixed string
            al = "glsx"
        if al:
            a2e[al.lower()] = ent
            e2a[ent_l] = al
    return a2e, e2a

def normalize_side(name, a2e):
    """
    Map a token like 'mas' or 'glsx' to:
      (entity_name, alias_to_show)
    using only what is present in v7c.json.
    """
    n = (name or "").strip()
    if not n:
        return n, None
    key = n.lower()
    if key in a2e:
        ent = a2e[key]
        return ent, n
    # no alias mapping: treat it as an entity without alias
    return n, None

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
    return joins

def mine_equality_predicates(text):
    preds = []
    for m in EQ_RE.finditer(text):
        preds.append(("EQ", m.group(1), m.group(2), m.group(3), m.group(4)))
    for m in SUBSTR_EQ_RE.finditer(text):
        preds.append(("SUBSTR_EQ", m.group(1), m.group(2), m.group(3), m.group(4)))
    return preds

def infer_joins(v7, base_entity, debug=False):
    """
    Build joins ONLY from information already present in v7c:
      - join_logic
      - business_rules
      - candidate_where_predicates
      - derived_columns.expression

    No hardcoded entities/columns. The only "fixed" piece is the
    alias name 'glsx' for the glsxref-like entity, as per your request.
    """
    a2e, e2a = alias_map(v7)

    # 1) Start with whatever joins the developer already wrote
    explicit = extract_explicit_joins(v7)

    # 2) Mine additional joins from all the free-form SQL text we already parsed
    haystacks = []
    for _, meta in v7.items():
        haystacks += (meta.get("join_logic") or [])
        haystacks += (meta.get("business_rules") or [])
        haystacks += (meta.get("candidate_where_predicates") or [])
        for d in meta.get("derived_columns") or []:
            expr = d.get("expression")
            if expr:
                haystacks.append(expr)

    auto_joins = []
    for txt in haystacks:
        for kind, a1, c1, a2, c2 in mine_equality_predicates(txt):
            left_ent, left_alias = normalize_side(a1, a2e)
            right_ent, right_alias = normalize_side(a2, a2e)

            if not left_ent or not right_ent:
                continue

            if kind == "EQ":
                auto_joins.append(
                    render_join(left_ent, left_alias, c1, right_ent, right_alias, c2)
                )
            elif kind == "SUBSTR_EQ":
                L = f"{left_alias}" if left_alias else f"{left_ent}"
                R = f"{right_alias}" if right_alias else f"{right_ent}"
                auto_joins.append(
                    f"LEFT JOIN {right_ent}{(' ' + right_alias) if right_alias else ''} "
                    f"ON substring({L}.{c1}, 4, 5) = {R}.{c2}"
                )

    # 3) Merge and dedupe (case-insensitive, whitespace-insensitive)
    def norm(j):
        return re.sub(r"\s+", " ", j.strip()).lower()

    final = []
    seen = set()
    for j in explicit + auto_joins:
        key = norm(j)
        if key in seen:
            continue
        seen.add(key)
        final.append(j)

    if debug:
        print("---- inferred joins ----")
        for j in final:
            print(j)

    return final

def build_model(v7, debug=False):
    base = choose_base_entity(v7)
    joins = infer_joins(v7, base, debug)

    # Business rules (as-provided)
    rules = []
    for meta in v7.values():
        rules += meta.get("business_rules", []) or []
        rules += meta.get("candidate_where_predicates", []) or []
    rules = uniq(rules)

    # Derived: dedupe by target name, prefer longer expression
    derived_map = {}  # tgt_lower -> entry
    idx = 0
    for meta in v7.values():
        for d in meta.get("derived_columns", []) or []:
            tgt = d.get("name")
            expr = d.get("expression", "") or ""
            if not tgt:
                continue
            key = tgt.strip().lower()
            entry = {
                "target": tgt,
                "expression": expr,
                "datatype": infer_datatype(expr),
                "_idx": idx,
            }
            idx += 1
            if key not in derived_map:
                derived_map[key] = entry
                continue
            prev = derived_map[key]
            prev_expr = prev.get("expression") or ""
            if len(expr.strip()) > len(prev_expr.strip()):
                entry["_idx"] = prev["_idx"]
                derived_map[key] = entry

    derived = [v for _, v in sorted(derived_map.items(), key=lambda kv: kv[1]["_idx"])]
    for d in derived:
        d.pop("_idx", None)

    # Statics: dedupe by (target, value) and wrap in CAST / TRY_CAST
    seen_static = set()
    statics = []
    for meta in v7.values():
        for s in meta.get("static_assignments", []) or []:
            tgt = s.get("target_column")
            val = s.get("value", "")
            key = (tgt or "").lower() + "|" + str(val).strip().lower()
            if key in seen_static:
                continue
            seen_static.add(key)
            dtype = infer_datatype(val)
            expr = build_cast_expression(val, dtype)
            statics.append(
                {
                    "target": tgt,
                    "expression": expr,
                    "datatype": dtype,
                }
            )

    return {
        "base_entity": base,
        "joins": joins,
        "business_rules": rules,
        "derived": derived,
        "statics": statics,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="path to source_columns_summary_v7c.json")
    ap.add_argument("--output", required=True, help="path to write model_v1.json")
    ap.add_argument("--debug-joins", action="store_true")
    args = ap.parse_args()

    v7 = read_json(args.input)
    model = build_model(v7, args.debug_joins)

    out_dir = os.path.dirname(args.output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(model, f, indent=2)

    print(f"✅ Model written to {args.output}")
    print(f"Base entity: {model['base_entity']}")
    print(f"Joins: {len(model['joins'])}, Derived: {len(model['derived'])}, Statics: {len(model['statics'])}")

if __name__ == "__main__":
    main()
