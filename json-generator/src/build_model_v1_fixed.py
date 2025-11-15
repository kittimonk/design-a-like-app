#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Reads a refined v7c JSON (source_columns_summary_v7c.json),
and produces model_v1_fixed.json with:
  - base_entity (auto-detected)
  - joins (deduped, normalized)
  - business_rules (deduped, SQL-only)
  - columns (unique targets from derived + statics)
    * static assignments casted to their target datatype
    * decimal statics use DECIMAL(17,2) with NULL-safe fallback
    * derived expressions left as-is (no casts here; casting is better
      applied later when we know source vs target types from the mapping CSV)
No hardcoded entity/table/column/alias names.
"""

import argparse, json, re, sys
from collections import defaultdict, OrderedDict
from pathlib import Path

# ---------- Helpers ----------------------------------------------------------

DECIMAL_STD = "DECIMAL(17,2)"

def pretty(obj) -> str:
    return json.dumps(obj, indent=2, ensure_ascii=False)

def is_sql_like(s: str) -> bool:
    if not isinstance(s, str):
        return False
    t = s.strip()
    if not t:
        return False
    # very light heuristic: contains uppercase SQL keywords / functions / case
    return bool(re.search(r"\b(case|when|then|else|end|select|coalesce|cast|try_cast|substr|substring|left|right|rtrim|ltrim|concat|current_timestamp|to_date|get_stndrd_id)\b", t, re.IGNORECASE))

def clean_business_rule(s: str) -> str | None:
    """Keep only SQL-like rules; strip bullet/numbering noise."""
    if not isinstance(s, str):
        return None
    t = s.strip()
    if not t:
        return None
    # remove leading numbering like "1) " or "• "
    t = re.sub(r"^\s*(\d+\)|[-*•]\s+)", "", t)
    # keep only if it looks SQLy
    return t if is_sql_like(t) else None

def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip()) if isinstance(s, str) else s

def dedupe_preserve_order(seq):
    seen = set()
    out = []
    for x in seq:
        k = normalize_space(x)
        if k and k not in seen:
            seen.add(k)
            out.append(x)
    return out

def infer_datatype_from_expr(expr: str) -> str | None:
    """Very light inference for derived expressions."""
    if not isinstance(expr, str):
        return None
    s = expr.lower()
    # dates
    if "to_date(" in s or re.search(r"try_cast\(.* as date\)", s):
        return "DATE"
    # timestamps
    if "current_timestamp" in s or re.search(r"try_cast\(.* as timestamp\)", s):
        return "TIMESTAMP"
    # decimals / numeric
    if re.search(r"\bdecimal\(", s) or re.search(r"try_cast\(.* as (double|decimal)", s):
        return DECIMAL_STD
    # integer-ish (best effort)
    if re.search(r"\b(as|try_cast)\s+\b(bigint|int|int32|int64)\b", s):
        return "BIGINT"
    # strings default
    return None

def cast_static(value: str, target_dt: str) -> str:
    """For static assignments only: cast constant to target type.
       If target is DECIMAL(17,2), add a NULL-safe fallback."""
    t = (target_dt or "").strip().upper()
    v = str(value).strip()

    # quote single bare strings that look like identifiers or +00000 codes
    is_quoted = (v.startswith("'") and v.endswith("'")) or (v.startswith('"') and v.endswith('"'))
    looks_numeric = re.fullmatch(r"[-+]?\d+(\.\d+)?", v) is not None

    if t.startswith("DECIMAL"):
        # Use our standard decimal precision if not specified
        if t == "DECIMAL":
            t = DECIMAL_STD
        # ensure numeric literal is castable; if not numeric, wrap in quotes
        if not looks_numeric and not is_quoted and v.lower() not in ("null",):
            v = f"'{v}'"
        return f"COALESCE(TRY_CAST({v} AS {t}), TRY_CAST(NULL AS {t}))"

    if t in ("BIGINT", "INT", "INT32", "INT64"):
        if not looks_numeric and not is_quoted and v.lower() not in ("null",):
            v = f"'{v}'"
        return f"CAST({v} AS {t})"

    if t in ("DATE", "TIMESTAMP"):
        # If user passed 'null', let it be NULL
        if v.lower() == "null":
            return f"CAST(NULL AS {t})"
        # If looks like to_date(...) already, leave it
        if re.search(r"\bto_date\s*\(", v, re.IGNORECASE):
            return v
        # Otherwise cast literal/string
        if not is_quoted and not looks_numeric and v.lower() not in ("null",):
            v = f"'{v}'"
        return f"CAST({v} AS {t})"

    # Default STRING
    if not is_quoted and v.lower() not in ("null",) and not looks_numeric:
        v = f"'{v}'"
    return f"CAST({v} AS STRING)"

def ensure_case_end(expr: str) -> str:
    """If expression begins a CASE without END, append END."""
    if not isinstance(expr, str):
        return expr
    txt = expr.strip()
    has_case = re.search(r"\bcase\b", txt, re.IGNORECASE)
    has_end  = re.search(r"\bend\b",  txt, re.IGNORECASE)
    if has_case and not has_end:
        return txt + " END"
    return txt

# ---------- Core -------------------------------------------------------------

def detect_base_entity(v7c: dict) -> str:
    """Pick entity with most derived+statics; fallback to the max referenced columns."""
    best_name, best_score = None, -1
    for name, block in v7c.items():
        dcount = len(block.get("derived_columns", []) or [])
        scount = len(block.get("static_assignments", []) or [])
        score  = dcount + scount
        if score > best_score:
            best_name, best_score = name, score
    if best_name:
        return best_name
    # fallback: most referenced_columns
    best_name, best_rc = None, -1
    for name, block in v7c.items():
        rc = len(block.get("referenced_columns", []) or [])
        if rc > best_rc:
            best_name, best_rc = name, rc
    return best_name or next(iter(v7c.keys()))

def collect_joins(v7c: dict) -> list[str]:
    all_joins = []
    for name, block in v7c.items():
        for j in block.get("join_logic", []) or []:
            j1 = normalize_space(j)
            if j1:
                all_joins.append(j1)
    return dedupe_preserve_order(all_joins)

def collect_business_rules(v7c: dict) -> list[str]:
    out = []
    for _, block in v7c.items():
        for sec in ("business_rules", "candidate_where_predicates"):
            for r in block.get(sec, []) or []:
                cleaned = clean_business_rule(r)
                if cleaned:
                    out.append(normalize_space(cleaned))
    return dedupe_preserve_order(out)

def collect_columns(v7c: dict) -> list[dict]:
    """
    Build unique target columns from derived_columns (name/expression) across entities
    + static_assignments (target_column/value).  No duplicates by target key.
    """
    by_target = OrderedDict()

    # 1) derived columns first (entity order preserved)
    for _, block in v7c.items():
        for d in block.get("derived_columns", []) or []:
            name = (d.get("name") or "").strip()
            expr = (d.get("expression") or "").strip()
            if not name or not expr:
                continue
            key = name.lower()
            if key in by_target:
                continue
            expr = ensure_case_end(expr)
            # datatype inference (soft)
            dt = infer_datatype_from_expr(expr)
            by_target[key] = {
                "target": name,
                "expression": expr,
                **({"datatype": dt} if dt else {})
            }

    # 2) static assignments (later wins only if target not already covered)
    for _, block in v7c.items():
        for s in block.get("static_assignments", []) or []:
            tgt = (s.get("target_column") or "").strip()
            val = s.get("value")
            if not tgt:
                continue
            key = tgt.lower()
            if key in by_target:
                continue  # derived takes precedence
            # we try to infer datatype if the value hints at it; default STRING
            # but statics are where we do casting now
            inferred_dt = None
            if isinstance(val, str):
                low = val.lower()
                if "to_date(" in low:
                    inferred_dt = "DATE"
                elif low == "current_timestamp()" or "timestamp" in low:
                    inferred_dt = "TIMESTAMP"
                elif re.fullmatch(r"[-+]?\d+(\.\d+)?", val) is not None:
                    inferred_dt = DECIMAL_STD if "." in val else "BIGINT"

            # if developer wrote a function like to_date('...'), keep it raw (don’t wrap again)
            if isinstance(val, str) and ("to_date(" in val.lower()):
                expr = val
                dt   = inferred_dt or "DATE"
            else:
                # For statics, we must emit CASTs now
                dt   = inferred_dt or "STRING"
                expr = cast_static(str(val), dt)

            by_target[key] = {
                "target": tgt,
                "expression": expr,
                "datatype": dt
            }

    # Final ordered list
    return list(by_target.values())

def build_model(v7c_path: Path) -> dict:
    v7c = json.loads(v7c_path.read_text(encoding="utf-8"))

    base_entity = detect_base_entity(v7c)
    joins        = collect_joins(v7c)
    rules        = collect_business_rules(v7c)
    columns      = collect_columns(v7c)

    model = {
        "base_entity": base_entity,       # optional, can be ignored downstream
        "joins": joins,                   # deduped list of SQL join fragments
        "business_rules": rules,          # deduped list of SQL-ready where/qualify predicates
        "columns": columns                # [{target, expression, datatype?}, ...]
    }
    return model

# ---------- CLI --------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--v7c", required=True, help="Path to source_columns_summary_v7c.json")
    ap.add_argument("--out", required=True, help="Path to write model_v1_fixed.json")
    args = ap.parse_args()

    v7c_path = Path(args.v7c)
    out_path = Path(args.out)

    model = build_model(v7c_path)
    out_path.write_text(pretty(model), encoding="utf-8")
    print(f"Wrote {out_path} with {len(model['columns'])} columns, "
          f"{len(model['joins'])} joins, {len(model['business_rules'])} rules.")
    # Show a small preview
    preview_cols = model["columns"][:5]
    print("\nPreview (first 5 columns):")
    print(pretty(preview_cols))

if __name__ == "__main__":
    sys.exit(main())
