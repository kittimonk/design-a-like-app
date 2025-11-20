#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import argparse
import os
import re
from pathlib import Path

# For finding table.column in expressions
QUAL_ID_RX = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")


def read_json(path: str):
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
    Returned as a list of (expression, target).
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
    Returned as a list of (expression, target).
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


def find_first_qualified_column(expr: str) -> str:
    """
    Return the first 'alias.column' found in an expression, or "" if none.
    Used as the lookup join driver (generic, data-driven).
    """
    if not expr:
        return ""
    m = QUAL_ID_RX.search(expr)
    if not m:
        return ""
    return f"{m.group(1)}.{m.group(2)}"


def parse_lookup_mapping(lookup_txt_path: str):
    """
    Parse lookup_reference_module_v3.txt-style file and build a mapping:

        cd_name (lowercase) -> lookup_view_name

    Example block:

        lk_mcb_cd: {
            sql: \"\"\"
            ...
            and stndrd_cd_name in ('curncy_cd', 'int_freq_cd', 'tantrum_family_cd')
            )\"\"\",
            ...
        }

    For this block we map:
        "curncy_cd"         -> "lk_mcb_cd"
        "int_freq_cd"       -> "lk_mcb_cd"
        "tantrum_family_cd" -> "lk_mcb_cd"
    """
    if not lookup_txt_path:
        return None

    text = Path(lookup_txt_path).read_text(encoding="utf-8")

    # Find blocks of the form: <lk_name>: { ... stndrd_cd_name in (...) ... }
    pattern = re.compile(
        r"(lk_[A-Za-z0-9_]+_cd)\s*:\s*\{.*?stndrd_cd_name\s+in\s*\((.*?)\)",
        flags=re.IGNORECASE | re.DOTALL,
    )

    cd_to_view = {}

    for view_name, cd_list_str in pattern.findall(text):
        # Extract 'curncy_cd', 'int_freq_cd', ... from the parentheses
        cds = re.findall(r"'([^']+)'", cd_list_str)
        for cd in cds:
            cd_to_view[cd.lower()] = view_name

    return cd_to_view or None


def build_sql_from_model(model, v7=None, malcode="mcb", lookup_map=None):
    """
    Turn model_v1.json + optional v7c + optional lookup mapping into a flat SQL view payload.

    Behaviour (Option C + multi lookup):
    - Only target columns ending with '_cd' are candidates.
    - If that target has a static assignment → NO lookup, keep static as-is.
    - If lookup_map is provided (parsed from lookup_reference_module_v3.txt),
      we only build lookups for *_cd columns that appear in lookup_map.
        * cd_name 'tantrum_family_cd' → whichever view in lookup_map has it (e.g., lk_mcb_cd)
        * cd_name 'int_freq_cd'       → could be lk_mcb_cd or lk_aaw_cd, depending on mapping
    - If lookup_map is NOT provided, we fall back to a single view lk_<malcode>_cd
      (backwards compatible with earlier behaviour).
    - All lookup LEFT JOINs are appended AFTER existing source joins.
    """
    base_entity = model.get("base_entity")
    if not base_entity:
        raise ValueError("model_v1.json missing 'base_entity'")

    v7 = v7 or {}

    # Alias info from v7c if available
    ent_to_alias = alias_map_from_v7(v7)
    base_alias = ent_to_alias.get(base_entity)

    # SELECT list pieces
    passthrough = gather_passthrough(v7, base_entity, base_alias)
    derived = gather_derived(model)
    statics = gather_statics(model)

    # Targets which already have a static assignment → NO lookup for these _cd columns
    static_targets = {tgt.lower() for _, tgt in statics}

    select_items = []
    lookup_specs = []  # each: {target, target_low, expr, driver, alias, view}

    # 1) Pass-through columns (unchanged)
    for expr, tgt in passthrough:
        select_items.append(f"    {expr} AS {tgt}")

    # 2) Derived columns, with lookup wrapping for *_cd without static assignment
    for expr, tgt in derived:
        tgt_low = (tgt or "").lower()

        # Decide which lookup view (if any) to use for this target
        views_for_cd = []

        if tgt_low.endswith("_cd") and tgt_low not in static_targets:
            if lookup_map is None:
                # No mapping file → fall back to single lookup view per malcode (old behaviour)
                views_for_cd = [f"lk_{malcode.lower()}_cd"]
            else:
                # Mapping file present → only use view if mapping exists for this specific cd
                view_name = lookup_map.get(tgt_low)
                if view_name:
                    views_for_cd = [view_name]

        if views_for_cd:
            # For now we assume 1 view per cd_name is the normal pattern.
            view_name = views_for_cd[0]
            driver = find_first_qualified_column(expr)
            if driver:
                lk_alias = f"{view_name}_{len(lookup_specs) + 1}"
                lookup_specs.append(
                    {
                        "target": tgt,
                        "target_low": tgt_low,
                        "expr": expr,
                        "driver": driver,
                        "alias": lk_alias,
                        "view": view_name,
                    }
                )
                wrapped_expr = (
                    f"CAST(CASE WHEN {lk_alias}.stndrd_cd_name = '{tgt_low}' "
                    f"THEN {lk_alias}.stndrd_cd_value ELSE {expr} END AS STRING)"
                )
                select_items.append(f"    {wrapped_expr} AS {tgt}")
            else:
                # No driver found → fall back to original expression (no lookup)
                select_items.append(f"    {expr} AS {tgt}")
        else:
            # Not a _cd column, or has static assignment, or no mapping → no lookup involvement
            select_items.append(f"    {expr} AS {tgt}")

    # 3) Static assignments (unchanged)
    for expr, tgt in statics:
        select_items.append(f"    {expr} AS {tgt}")

    if not select_items:
        # Fallback: if we somehow have no columns, select everything from base
        base_ref = base_alias or base_entity
        select_items.append(f"    {base_ref}.*")

    select_clause = "SELECT\n" + ",\n".join(select_items)

    # FROM clause (unchanged)
    if base_alias:
        from_clause = f"FROM {base_entity} {base_alias}"
    else:
        from_clause = f"FROM {base_entity}"

    # Joins from model_v1.json (unchanged)
    joins = model.get("joins", []) or []
    join_lines = [j.strip() for j in joins if j and j.strip()]

    # NEW: lookup LEFT JOINs after all source joins (Option A)
    for spec in lookup_specs:
        lk_table = spec["view"]          # e.g. lk_mcb_cd OR lk_aaw_cd
        lk_alias = spec["alias"]         # e.g. lk_mcb_cd_1
        driver = spec["driver"]          # e.g. mas.srsectype
        tgt_low = spec["target_low"]     # e.g. "tantrum_family_cd"

        join_lines.append(
            f"LEFT JOIN {lk_table} {lk_alias} "
            f"ON {driver} = {lk_alias}.source_value1 "
            f"AND {lk_alias}.stndrd_cd_name = '{tgt_low}'"
        )

    join_clause = ""
    if join_lines:
        join_clause = "\n" + "\n".join(join_lines)

    # Business rules → WHERE + QUALIFY (unchanged)
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
        help="Path to model_v1.json produced by build_model_v1_v11c.py",
    )
    ap.add_argument(
        "--v7c",
        required=False,
        help="Optional path to source_columns_summary_v7c.json "
             "for passthrough column discovery and lookup decisions",
    )
    ap.add_argument(
        "--lookup-txt",
        required=False,
        help="Optional path to lookup_reference_module_v3.txt for cd_name -> lookup view mapping",
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
    lookup_map = parse_lookup_mapping(args.lookup_txt) if args.lookup_txt else None

    payload = build_sql_from_model(model, v7, malcode=args.malcode, lookup_map=lookup_map)

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
