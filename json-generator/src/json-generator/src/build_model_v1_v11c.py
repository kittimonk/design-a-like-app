import json, re, argparse, os
from collections import defaultdict

EQ_RE = re.compile(r"\b([A-Za-z_]\w*)\.(\w+)\s*=\s*([A-Za-z_]\w*)\.(\w+)")
SUBSTR_EQ_RE = re.compile(
    r"substring\(\s*([A-Za-z_]\w*)\.(\w+)\s*,\s*\d+\s*,\s*\d+\s*\)\s*=\s*([A-ZaLz_]\w*)\.(\w+)",
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

def infer_datatype(expr):
    e = (expr or "").lower()
    if "to_date(" in e: return "DATE"
    if "current_timestamp" in e: return "TIMESTAMP"
    if "decimal" in e or re.search(r"\d+\.\d+", e): return "DECIMAL(17,2)"
    m = re.search(r"try_cast\([^)]* as ([a-z0-9_()]+)\)", e)
    if m: return m.group(1).upper()
    if re.search(r"[+-]?\d+\b", e): return "BIGINT"
    return "STRING"

def build_cast_expression(value, dtype):
    v = str(value).strip()
    if re.match(r"to_date|current_timestamp", v, re.I):
        return f"CAST({v} AS {dtype})"
    if dtype.startswith("DECIMAL"):  # numeric with scale
        return f"COALESCE(TRY_CAST({v} AS {dtype}), TRY_CAST(NULL AS {dtype}))"
    if dtype in ("BIGINT","INT","DOUBLE","FLOAT"):
        return f"TRY_CAST({v} AS {dtype})"
    return f"CAST({v} AS {dtype})"

def choose_base_entity(v7):
    scored = [(len(m.get("derived_columns", [])), ent) for ent, m in v7.items()]
    scored.sort(reverse=True)
    return scored[0][1] if scored else list(v7.keys())[0]

def alias_map(v7):
    """alias -> entity, and entity -> alias (when present)"""
    a2e = {}
    e2a = {}
    for ent, meta in v7.items():
        al = (meta.get("alias") or "").strip()
        if al:
            a2e[al.lower()] = ent
            e2a[ent.lower()] = al
    return a2e, e2a

def normalize_side(name, a2e):
    """Return (entity_name, alias_to_show) using the alias map if available."""
    n = name.lower()
    if n in a2e:
        ent = a2e[n]
        return ent, n  # entity, alias
    return name, None  # treat as entity without alias

def render_join(left_ent, left_alias, left_col, right_ent, right_alias, right_col, prefer_left_alias=True):
    L = f"{left_alias}" if left_alias else f"{left_ent}"
    R = f"{right_alias}" if right_alias else f"{right_ent}"
    # Keep LEFT JOIN direction stable but do not enforce which side is base; caller decides
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

def infer_joins(v7, base_entity, debug=False):
    a2e, e2a = alias_map(v7)

    # 1) Start with explicit joins from v7c (authoritative, already curated)
    joins = extract_explicit_joins(v7)

    # Normalize explicit joins: if the explicit join lists the base entity as the joined table
    # (i.e. joins ossbr_2_1 mas ...) but the ON clause relates mas to another table (glsx),
    # flip the join so it reads as joining the other table. This avoids self-joins caused by
    # author-entered joins that used the wrong side.
    normalized = []
    for j in joins:
        m = re.search(r"(?i)\bjoin\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)(?:\s+([A-Za-z_]\w*))?\s+on\s+(.+)$", j.strip())
        if m:
            join_table = m.group(1)
            join_alias = m.group(2)
            on_clause = m.group(3)
            em = EQ_RE.search(on_clause)
            if em:
                a1 = em.group(1); a2 = em.group(3)
                # If the declared join table is actually the base entity (or its alias),
                # switch to joining the other side (keep ON clause as-is so it reads
                # e.g. ON mas.col = glsx.col)
                jt = (join_table or '').lower()
                be = base_entity.lower()
                if jt == be or (join_alias and join_alias.lower() == be):
                    # determine the other side (alias) used in the ON clause
                    other_alias = a1 if a1.lower() != (join_alias.lower() if join_alias else jt) else a2
                    other_ent = a2e.get(other_alias.lower(), other_alias)
                    # build a normalized LEFT JOIN string (construct explicitly to avoid double 'LEFT')
                    new_join = f"LEFT JOIN {other_ent}{(' ' + other_alias) if other_alias else ''} ON {on_clause.strip()}"
                    normalized.append(new_join)
                    continue
        normalized.append(j)

    joins = normalized

    # 2) Mine additional join predicates from *your* text (no assumptions)
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

    # Turn predicates into LEFT JOINs, preferring base_entity on the left when present in the predicate
    auto_joins = []
    for kind, a1, c1, a2, c2 in mined:
        left_ent, left_alias = normalize_side(a1, a2e)
        right_ent, right_alias = normalize_side(a2, a2e)

        # Prefer base on the left; if not, keep discovered order
        if left_ent.lower() != base_entity.lower() and right_ent.lower() == base_entity.lower():
            # swap sides to keep base on the left for readability; still no hardcoding
            left_ent, right_ent = right_ent, left_ent
            left_alias, right_alias = right_alias, left_alias
            c1, c2 = c2, c1

        if kind == "EQ":
            auto_joins.append(render_join(left_ent, left_alias, c1, right_ent, right_alias, c2))
        elif kind == "SUBSTR_EQ":
            # keep the substring side on the left in the ON clause
            L = f"{left_alias}" if left_alias else f"{left_ent}"
            R = f"{right_alias}" if right_alias else f"{right_ent}"
            auto_joins.append(f"LEFT JOIN {right_ent}{(' ' + right_alias) if right_alias else ''} "
                              f"ON substring({L}.{c1}, 4, 5) = {R}.{c2}")

    # Merge & dedupe; do NOT invent srseccode or any default keys
    joins = uniq(joins + auto_joins)

    # Further normalize/dedupe joins: prefer the join that includes an alias when two joins
    # reference the same target entity and equivalent ON clause.
    # Helper: canonicalize ON clause by mapping aliases/entities to a canonical alias when possible
    def canonicalize_on(on_clause):
        def repl(m):
            tbl = m.group(1)
            col = m.group(2)
            tl = tbl.lower()
            # if tbl is an alias, map to its entity then to canonical alias
            if tl in a2e:
                ent = a2e[tl]
                canonical = e2a.get(ent.lower(), tl)
            else:
                # tbl might be an entity name; prefer its alias when available
                canonical = e2a.get(tl, tbl)
            return f"{canonical}.{col}"
        return re.sub(r"\b([A-Za-z_]\w*)\.(\w+)", repl, on_clause).lower().strip()

    final = []
    seen = {}
    for j in joins:
        m = re.search(r"(?i)\bjoin\s+([A-Za-z_]\w*(?:\.[A-Za-z_]\w*)?)(?:\s+([A-Za-z_]\w*))?\s+on\s+(.+)$", j.strip())
        if not m:
            key = j.strip().lower()
            if key in seen:
                continue
            seen[key] = j
            final.append(j)
            continue
        right_ent = m.group(1).lower()
        right_alias = (m.group(2) or '').lower()
        # normalize ON clause by canonicalizing table/alias tokens so 'mfsp.col' and 'mfspric.col' match
        on_norm = re.sub(r"\s+", " ", canonicalize_on(m.group(3)))
        key = (right_ent, on_norm)
        prev = seen.get(key)
        if prev:
            # if current has alias and previous doesn't, replace
            prev_has_alias = bool(re.search(r"(?i)\bjoin\s+[A-Za-z0-9_.]+\s+[A-Za-z_]\w*\b", prev))
            cur_has_alias = bool(m.group(2))
            if prev_has_alias:
                continue
            if cur_has_alias:
                idx = final.index(prev)
                final[idx] = j
                seen[key] = j
                continue
            # both lack alias or both have alias, keep first
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

    # Business rules (as-provided)
    rules = []
    for meta in v7.values():
        rules += meta.get("business_rules", []) or []
        rules += meta.get("candidate_where_predicates", []) or []
    rules = uniq(rules)

    # Derived
    # Collect derived columns and dedupe by target name. When multiple expressions
    # exist for the same target, prefer the one with the longer expression (more
    # likely to be complete) or the first non-empty expression.
    derived_map = {}  # tgt_lower -> {target, expression, datatype, first_index}
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
                continue
            # Resolve conflict: pick entry with longer expression, or if equal
            # prefer the one seen earlier.
            prev = derived_map[key]
            prev_expr = (prev.get("expression") or "")
            if len(expr.strip()) > len(prev_expr.strip()):
                # replace
                entry["_idx"] = prev["_idx"]  # keep original position
                derived_map[key] = entry

    # Convert map back to list preserving original first-seen order
    derived = [v for k, v in sorted(derived_map.items(), key=lambda kv: kv[1]["_idx"]) ]
    for d in derived:
        # remove internal index before finalizing
        d.pop("_idx", None)

    # Statics
    seen_static = set(); statics = []
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
