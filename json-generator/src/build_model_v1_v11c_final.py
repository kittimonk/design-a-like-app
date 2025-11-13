import json, re, argparse, os

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

def infer_datatype(expr_or_value):
    e = (str(expr_or_value) if expr_or_value is not None else "").lower()
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
    """Build alias <-> entity maps from the JSON metadata."""
    a2e = {}  # alias -> entity
    e2a = {}  # entity -> alias
    for ent, meta in v7.items():
        al = (meta.get("alias") or "").strip()
        if al:
            a2e[al.lower()] = ent
            e2a[ent.lower()] = al
    return a2e, e2a

def normalize_side(name, a2e):
    """
    Given a name (could be alias or entity), return (entity_name, alias_to_show)
    using alias_map when possible.
    """
    n = name.lower()
    if n in a2e:
        ent = a2e[n]
        return ent, n  # entity, alias
    return name, None

def render_join(left_ent, left_alias, left_col, right_ent, right_alias, right_col):
    L = left_alias if left_alias else left_ent
    R = right_alias if right_alias else right_ent
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
    for m in EQ_RE.finditer(text or ""):
        preds.append(("EQ", m.group(1), m.group(2), m.group(3), m.group(4)))
    for m in SUBSTR_EQ_RE.finditer(text or ""):
        preds.append(("SUBSTR_EQ", m.group(1), m.group(2), m.group(3), m.group(4)))
    return preds

def parse_join_clause(j):
    """
    Parse a join string into (join_entity, join_alias, on_clause) or (None, None, None)
    if it doesn't match a normal JOIN pattern. Case-insensitive, generic.
    """
    m = re.search(
        r"(?i)\bjoin\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)"  # entity or db.entity
        r"(?:\s+([A-Za-z_]\w*))?"                         # optional alias
        r"\s+on\s+(.+)$",                                # ON ...
        j.strip()
    )
    if not m:
        return None, None, None
    join_ent = m.group(1)
    join_alias = m.group(2)
    on_clause = m.group(3)
    return join_ent, join_alias, on_clause

def canonicalize_on(on_clause, a2e, e2a):
    """
    Canonicalize ON clause by mapping alias/entity tokens to a canonical alias:
    - If token is an alias, map to its entity, then to that entity's preferred alias.
    - If token is an entity name, map to its preferred alias when available.
    This is generic; no hardcoded table names.
    """
    def repl(m):
        tbl = m.group(1)
        col = m.group(2)
        tl = tbl.lower()

        if tl in a2e:
            ent = a2e[tl]
            canonical = e2a.get(ent.lower(), tl)
        else:
            canonical = e2a.get(tl, tbl)

        return f"{canonical}.{col}"

    return re.sub(r"\b([A-Za-z_]\w*)\.(\w+)", repl, on_clause).lower().strip()

def infer_joins(v7, base_entity, debug=False):
    a2e, e2a = alias_map(v7)

    # 1) Start from explicit joins
    explicit = extract_explicit_joins(v7)

    normalized = []
    base_lower = base_entity.lower()

    for j in explicit:
        join_ent, join_alias, on_clause = parse_join_clause(j)
        if not join_ent:
            normalized.append(j)
            continue

        jt = join_ent.split(".")[-1].lower()  # strip db. prefix if any
        ja = (join_alias or "").lower()

        # Look at the first simple equality in the ON clause to see which aliases are involved
        em = EQ_RE.search(on_clause or "")
        if em:
            a1 = em.group(1)  # left alias in ON
            a2 = em.group(3)  # right alias in ON
            a1_l = a1.lower()
            a2_l = a2.lower()

            # Determine which side is effectively the base (entity or alias)
            base_aliases = {name for name, ent in a2e.items() if ent.lower() == base_lower}

            # The alias actually used to represent the base in ON, if any
            base_token = None
            if a1_l in base_aliases or a1_l == base_lower:
                base_token = a1
            elif a2_l in base_aliases or a2_l == base_lower:
                base_token = a2

            # If the JOIN clause is written "join <base_entity> <alias>" but the ON clause
            # relates that alias to some OTHER table/alias, treat this as "join that OTHER table".
            if jt == base_lower or ja == base_lower or ja in base_aliases:
                if base_token is not None:
                    other_alias = a2 if base_token == a1 else a1
                    other_ent = a2e.get(other_alias.lower(), other_alias)
                    new_join = f"LEFT JOIN {other_ent}{(' ' + other_alias) if other_alias else ''} ON {on_clause.strip()}"
                    normalized.append(new_join)
                    continue

        # Otherwise keep as-is
        normalized.append(j)

    joins = normalized

    # 2) Mine additional join predicates from expressions, business rules, etc.
    haystacks = []
    for _, meta in v7.items():
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

            # Prefer base entity on the left for readability
            if left_ent.lower() != base_lower and right_ent.lower() == base_lower:
                left_ent, right_ent = right_ent, left_ent
                left_alias, right_alias = right_alias, left_alias
                c1, c2 = c2, c1

            if kind == "EQ":
                auto_joins.append(
                    render_join(left_ent, left_alias, c1, right_ent, right_alias, c2)
                )
            elif kind == "SUBSTR_EQ":
                L = left_alias if left_alias else left_ent
                R = right_alias if right_alias else right_ent
                auto_joins.append(
                    f"LEFT JOIN {right_ent}{(' ' + right_alias) if right_alias else ''} "
                    f"ON substring({L}.{c1}, 4, 5) = {R}.{c2}"
                )

    # Combine and dedupe
    joins = uniq(joins + auto_joins)

    # 3) Final normalization + dedupe:
    #    - Drop joins whose joined entity IS the base entity (self-join),
    #      since the base itself is the FROM table.
    #    - Prefer joins that have an alias over those without, when the ON
    #      condition is equivalent.
    final = []
    seen = {}  # key: (canonical_entity, canonical_on) -> join string

    for j in joins:
        join_ent, join_alias, on_clause = parse_join_clause(j)
        if not join_ent:
            key = j.strip().lower()
            if key in seen:
                continue
            seen[key] = j
            final.append(j)
            continue

        ent_name = join_ent.split(".")[-1]  # ignore db prefix
        ent_lower = ent_name.lower()

        # Skip self-joins against the base entity
        if ent_lower == base_lower:
            continue

        # Canonical entity key: use entity name only
        canonical_entity = ent_lower

        # Canonical ON clause (maps aliases/entities to canonical alias)
        on_norm = re.sub(r"\s+", " ", canonicalize_on(on_clause, a2e, e2a))

        key = (canonical_entity, on_norm)
        prev = seen.get(key)

        if prev:
            # Decide whether to keep previous or replace with current:
            # prefer the join that actually has an alias.
            prev_ent, prev_alias, _ = parse_join_clause(prev)
            prev_has_alias = bool(prev_alias)
            cur_has_alias = bool(join_alias)

            if prev_has_alias and not cur_has_alias:
                # keep previous
                continue
            if cur_has_alias and not prev_has_alias:
                # replace previous with current
                idx = final.index(prev)
                final[idx] = j
                seen[key] = j
                continue

            # Both have alias or both do not -> keep first.
            continue

        seen[key] = j
        final.append(j)

    joins = final

    if debug:
        print("---- inferred joins ----")
        for j in joins:
            print(j)
    return joins

def build_model(v7, debug=False):
    base = choose_base_entity(v7)
    joins = infer_joins(v7, base, debug)

    # Business rules (as provided)
    rules = []
    for meta in v7.values():
        rules += meta.get("business_rules", []) or []
        rules += meta.get("candidate_where_predicates", []) or []
    rules = uniq(rules)

    # Derived: dedupe by target name, prefer "richer" expression
    derived_map = {}
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
                "_idx": idx
            }
            idx += 1
            if key not in derived_map:
                derived_map[key] = entry
            else:
                prev = derived_map[key]
                prev_expr = (prev.get("expression") or "")
                if len(expr.strip()) > len(prev_expr.strip()):
                    entry["_idx"] = prev["_idx"]
                    derived_map[key] = entry

    derived = [v for _, v in sorted(derived_map.items(), key=lambda kv: kv[1]["_idx"])]
    for d in derived:
        d.pop("_idx", None)

    # Statics: dedupe by (target, value) and generate typed expressions
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
