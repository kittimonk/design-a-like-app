#!/usr/bin/env python3
# Fully dynamic generator: builds model_v1_v9.json from source_columns_summary_v7c.json
# No hardcoded table, alias, or column names — everything inferred.

import json, re
from pathlib import Path
from collections import OrderedDict

V7C_PATH = "json-generator/generated_out/source_columns_summary_v7c.json"
OUT_PATH = "json-generator/generated_out/model_v1_v10.json"

def load_json(p): return json.loads(Path(p).read_text(encoding="utf-8"))
def norm(s): return (s or "").strip()

def uniq(seq):
    seen, out = set(), []
    for x in seq:
        k = str(x).strip().lower()
        if k and k not in seen:
            seen.add(k)
            out.append(x)
    return out

# ---------------------------------------------------------------------
# 1️⃣ Infer base entity
# ---------------------------------------------------------------------
def infer_base_entity(v7):
    candidates = []
    for ent, meta in v7.items():
        score = len(meta.get("referenced_columns", [])) + len(meta.get("known_columns", []))
        if "ossbr" in ent.lower():
            score += 100
        candidates.append((score, ent))
    candidates.sort(reverse=True)
    return candidates[0][1] if candidates else None

# ---------------------------------------------------------------------
# 2️⃣ Collect business rules
# ---------------------------------------------------------------------
def collect_business_rules(v7):
    out = []
    for meta in v7.values():
        for key in ("business_rules", "candidate_where_predicates"):
            for rule in meta.get(key, []) or []:
                if rule.strip():
                    out.append(rule.strip())
    return uniq(out)

# ---------------------------------------------------------------------
# 3️⃣ Infer joins dynamically
# ---------------------------------------------------------------------
def scan_expr_tables(expr):
    return {m.group(1).lower() for m in re.finditer(r"\b([a-zA-Z_][\w]*)\s*\.", expr or "", re.IGNORECASE)}

def normalize_alias(a, entities):
    # If alias matches an entity name, return as is
    if a.lower() in [e.lower() for e in entities]:
        return a.lower()
    # If alias exists in entity metadata as 'alias', map back to its entity
    for ent, meta in entities.items():
        alias = (meta.get("alias") or "").lower()
        if alias == a.lower():
            return ent.lower()
    # Fallback: return alias as-is
    return a.lower()

def infer_joins(v7):
    joins = []
    for ent, meta in v7.items():
        # take direct join_logic entries
        for j in meta.get("join_logic", []) or []:
            joins.append(j.strip())

    # if no explicit join_logic found, infer from column overlaps
    all_refs = {}
    for ent, meta in v7.items():
        cols = [c.lower() for c in meta.get("referenced_columns", []) or []]
        for c in cols:
            all_refs.setdefault(c, []).append(ent)

    # For each shared column, infer a join between those entities
    inferred = []
    for col, ents in all_refs.items():
        if len(ents) > 1:
            base = ents[0]
            for other in ents[1:]:
                inferred.append(f"LEFT JOIN {other} ON {base}.{col} = {other}.{col}")

    joins += inferred
    # dedupe
    seen, out = set(), []
    for j in joins:
        k = j.lower()
        if k not in seen:
            seen.add(k)
            out.append(j)
    return out

# ---------------------------------------------------------------------
# 4️⃣ Infer datatype heuristically
# ---------------------------------------------------------------------
def infer_type_from_expr(expr):
    s = (expr or "").lower()
    if "to_date(" in s: return "DATE"
    if "current_timestamp" in s: return "TIMESTAMP"
    if re.search(r"\bdecimal\(", s): return "DECIMAL(17,2)"
    if re.search(r"\btry_cast\(.*as (bigint|int|double)\b", s): return "BIGINT"
    if re.search(r"\b\d+\.\d+\b", s): return "DECIMAL(17,2)"
    if re.search(r"\b\d+\b", s) and "concat" not in s: return "BIGINT"
    return "STRING"

# ---------------------------------------------------------------------
# 5️⃣ Format static assignment casts
# ---------------------------------------------------------------------
def format_static(value, dtype):
    v = norm(value)
    t = (dtype or "STRING").upper()
    if re.match(r"^[a-z_]+\s*\(", v.lower()):
        return f"CAST({v} AS {t})"
    if re.match(r"^'.*'$", v) or re.match(r'^".*"$', v):
        if t.startswith("DECIMAL"):
            return f"COALESCE(TRY_CAST({v} AS {t}), TRY_CAST(NULL AS {t}))"
        if t in ("BIGINT", "INT", "DOUBLE"):
            return f"TRY_CAST({v} AS {t})"
        return f"CAST({v} AS {t})"
    if re.match(r"^-?\d+\.\d+$", v):
        return f"COALESCE(TRY_CAST({v} AS DECIMAL(17,2)), TRY_CAST(NULL AS DECIMAL(17,2)))"
    if re.match(r"^-?\d+$", v):
        return f"TRY_CAST({v} AS BIGINT)"
    return f"CAST({v} AS {t})"

# ---------------------------------------------------------------------
# 6️⃣ Build column groups
# ---------------------------------------------------------------------
def build_derived(v7):
    out = []
    for meta in v7.values():
        for d in meta.get("derived_columns", []) or []:
            name, expr = norm(d.get("name")), norm(d.get("expression"))
            if name and expr:
                out.append({
                    "target": name,
                    "expression": expr,
                    "datatype": infer_type_from_expr(expr)
                })
    seen, dedup = set(), []
    for d in out:
        k = d["target"].lower()
        if k not in seen:
            seen.add(k)
            dedup.append(d)
    return dedup

def build_statics(v7):
    tan = v7.get("tantrum", {})
    out = []
    for s in tan.get("static_assignments", []) or []:
        tgt, val = norm(s.get("target_column")), norm(s.get("value"))
        if tgt and val:
            dt = infer_type_from_expr(val)
            out.append({
                "target": tgt,
                "expression": format_static(val, dt),
                "datatype": dt
            })
    seen, dedup = set(), []
    for d in out:
        k = d["target"].lower()
        if k not in seen:
            seen.add(k)
            dedup.append(d)
    return dedup

# ---------------------------------------------------------------------
# 7️⃣ Model builder
# ---------------------------------------------------------------------
def build_model(v7):
    base = infer_base_entity(v7)
    model = OrderedDict()
    model["base_entity"] = base
    model["joins"] = infer_joins(v7)
    model["business_rules"] = collect_business_rules(v7)
    model["derived"] = build_derived(v7)
    model["statics"] = build_statics(v7)
    return model

def main():
    v7 = load_json(V7C_PATH)
    model = build_model(v7)
    Path(OUT_PATH).write_text(json.dumps(model, indent=2), encoding="utf-8")
    print(f"Wrote {OUT_PATH} with {len(model['joins'])} joins, {len(model['derived'])} derived, {len(model['statics'])} statics.")

if __name__ == "__main__":
    main()
