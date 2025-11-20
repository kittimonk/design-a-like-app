import json
import argparse
import os
import re


def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def alias_map_from_v7(v7):
    """
    Build a simple entity -> alias map from source_columns_summary_v7c.json.
    If v7 does not have alias info, we just return {} and fall back to
    using the entity name directly.
    """
    ent_to_alias = {}
    for ent, meta in v7.items():
        al = (meta.get("alias") or "").strip() if isinstance(meta, dict) else ""
        if al:
            ent_to_alias[ent] = al
    return ent_to_alias


def gather_passthrough(v7, base_entity, base_alias):
    """
    Best-effort pass-through column discovery.

    This is intentionally defensive and fully data-driven:
    - If the v7c entry for the base entity has a key like `passthrough_columns`,
      we use those.
    - If not, we try to interpret common shapes like:
        { "source_column": "srseccode", "target_column": "srseccode" }
        { "expression": "mas.borid", "target": "sm_secrty_id" }
    - If the structure is not present, we simply return [] and the view will
      only contain derived + static targets (no hardcoding).
    """
    meta = v7.get(base_entity, {})
    if not isinstance(meta, dict):
        return []

    result = []

    # 1) Explicit passthrough list, if present
    passthrough_lists = []
    if "passthrough_columns" in meta and isinstance(meta["passthrough_columns"], list):
        passthrough_lists.append(meta["passthrough_columns"])
    # You can extend this if your v7c uses a different key for passthroughs.
    # For example: "direct_mappings" or "base_columns" etc.
    if "direct_mappings" in meta and isinstance(meta["direct_mappings"], list):
        passthrough_lists.append(meta["direct_mappings"])

    for lst in passthrough_lists:
        for col in lst:
            if not isinstance(col, dict):
                continue
            tgt = (
                col.get("target")
                or col.get("target_column")
                or col.get("name")
                or col.get("column")
            )
            expr = col.get("expression")

            # If expression is missing, try to build "alias.column" from source_column
            if not expr:
                src_col = (
                    col.get("source_column")
                    or col.get("source")
                    or col.get("source_name")
                    or tgt
                )
                if not src_col:
                    continue
                if base_alias:
                    expr = f"{base_alias}.{src_col}"
                else:
                    expr = f"{base_entity}.{src_col}"

            if not tgt:
                # Fallback: derive target from the expression's trailing identifier
                if "." in expr:
                    tgt = expr.split(".")[-1].strip()
            if not tgt:
                continue

            result.append((expr, tgt))

    # Deduplicate by target (case-insensitive)
    seen_targets = set()
    deduped = []
    for expr, tgt in result:
        key = tgt.lower()
        if key in seen_targets:
            continue
        seen_targets.add(key)
        deduped.append((expr, tgt))

    return deduped


def gather_derived(model):
    """
    Collect derived expressions from model_v1.json:
       [
         { "target": "...", "expression": "...", "datatype": "..." }, ...
       ]
    """
    out = []
    for d in model.get("derived", []):
        if not isinstance(d, dict):
            continue
        tgt = d.get("target")
        expr = d.get("expression")
        if tgt and expr:
            out.append((expr, tgt))
    return out


def gather_statics(model):
    """
    Collect static assignments from model_v1.json:
       [
         { "target": "...", "expression": "...", "datatype": "..." }, ...
       ]
    """
    out = []
    for s in model.get("statics", []):
        if not isinstance(s, dict):
            continue
        tgt = s.get("target")
        expr = s.get("expression")
        if tgt and expr:
            out.append((expr, tgt))
    return out


def split_rules_for_where_and_qualify(rules):
    """
    Option A: QUALIFY at the bottom.

    We treat any rule that:
      - starts with "qualify " (case-insensitive), OR
      - contains " row_number(" (common QUALIFY pattern)
    as a QUALIFY rule.

    Everything else goes into WHERE.
    """
    where_rules = []
    qualify_rules = []

    for r in rules or []:
        if not r:
            continue
        raw = r.strip()
        low = raw.lower()
        if low.startswith("qualify "):
            # strip the leading QUALIFY keyword; we'll re-add in the final clause
            cleaned = re.sub(r"(?i)^qualify\s+", "", raw).strip()
            if cleaned:
                qualify_rules.append(cleaned)
        elif " row_number(" in low:
            # treat as QUALIFY rule even if "QUALIFY" is missing in the text
            qualify_rules.append(raw)
        else:
            where_rules.append(raw)

    # Deduplicate preserving order
    def uniq(seq):
        seen = set()
        out = []
        for x in seq:
            k = x.strip().lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(x)
        return out

    return uniq(where_rules), uniq(qualify_rules)


def build_sql_from_model(model, v7=None, malcode="mcb"):
    """
    Turn model_v1.json + optional v7c into a flat SQL view payload.
    No hardcoded table names, aliases, or columns; everything comes from
    the JSON inputs and CLI args.
    """
    base_entity = model.get("base_entity")
    if not base_entity:
        raise ValueError("model_v1.json missing 'base_entity'")

    # Alias info from v7c if available
    ent_to_alias = alias_map_from_v7(v7 or {})
    base_alias = ent_to_alias.get(base_entity)

    # SELECT list pieces
    passthrough = gather_passthrough(v7 or {}, base_entity, base_alias)
    derived = gather_derived(model)
    statics = gather_statics(model)

    select_items = []

    # 1) Pass-through columns
    for expr, tgt in passthrough:
        select_items.append(f"    {expr} AS {tgt}")

    # 2) Derived
    for expr, tgt in derived:
        select_items.append(f"    {expr} AS {tgt}")

    # 3) Statics
    for expr, tgt in statics:
        select_items.append(f"    {expr} AS {tgt}")

    if not select_items:
        # Fallback: if we somehow have no columns, select everything from base
        base_ref = base_alias or base_entity
        select_items.append(f"    {base_ref}.*")

    select_clause = "SELECT\n" + ",\n".join(select_items)

    # FROM clause
    if base_alias:
        from_clause = f"FROM {base_entity} {base_alias}"
    else:
        from_clause = f"FROM {base_entity}"

    # Joins (as-is from model_v1.json)
    joins = model.get("joins", []) or []
    join_clause = ""
    if joins:
        # Ensure each join appears on its own line
        join_lines = [j.strip() for j in joins if j and j.strip()]
        join_clause = "\n" + "\n".join(join_lines)

    # Business rules → WHERE + QUALIFY
    all_rules = model.get("business_rules", []) or []
    where_rules, qualify_rules = split_rules_for_where_and_qualify(all_rules)

    where_clause = ""
    if where_rules:
        where_clause = "\nWHERE\n  " + "\n  AND ".join(where_rules)

    qualify_clause = ""
    if qualify_rules:
        qualify_clause = "\nQUALIFY\n  " + "\n  AND ".join(qualify_rules)

    # Assemble full SQL text
    sql = (
        select_clause
        + "\n"
        + from_clause
        + join_clause
        + where_clause
        + qualify_clause
    )

    # View name: dt_<malcode>_<base_entity>
    view_name = f"dt_{malcode.lower()}_{base_entity.lower()}"

    payload = {
        "sql": sql,
        "loggable": True,
        "options": {
            "module": "data_transformation",
            "method": "process"
        },
        "name": view_name
    }
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--model",
        required=True,
        help="Path to model_v1.json produced by final.py",
    )
    ap.add_argument(
        "--v7c",
        required=False,
        help="Optional path to source_columns_summary_v7c.json "
             "for passthrough column discovery",
    )
    ap.add_argument(
        "--malcode",
        required=True,
        help="Source malcode, e.g. MCB, ND, etc. Used to build dt_<malcode>_<base> name",
    )
    ap.add_argument(
        "--output",
        required=True,
        help="Path to write dt_<malcode>_<base>.json (the view payload)",
    )
    args = ap.parse_args()

    model = read_json(args.model)
    v7 = read_json(args.v7c) if args.v7c else {}

    payload = build_sql_from_model(model, v7, malcode=args.malcode)

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"✅ View payload written to {args.output}")
    print(f"Name: {payload['name']}")
    print("---- Preview SQL ----")
    print(payload["sql"][:800])
    print("... (truncated)")


if __name__ == "__main__":
    main()
