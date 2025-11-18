import argparse
import csv
import json
import os
import re
from typing import List, Optional, Tuple


def read_dt_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_view_info(dt: dict) -> Tuple[str, str, str]:
    """
    From dt JSON, get:
      - view_name  (e.g. 'dt_mcb_ossbr_2_1')
      - malcode    (e.g. 'mcb')
      - entity     (e.g. 'ossbr_2_1')
    """
    view_name = dt.get("name") or ""
    view_name = view_name.strip()

    m = re.match(r"^dt_([^_]+)_(.+)$", view_name)
    if not m:
        raise ValueError(f"Cannot parse malcode/entity from view name: {view_name!r}")

    malcode = m.group(1)
    entity = m.group(2)
    return view_name, malcode, entity


def extract_select_columns(sql: str) -> List[str]:
    """
    Extract the list of SELECT aliases (column names) from the dt SQL.

    Assumptions (true for your generated SQL):
      - One column per line between SELECT and FROM
      - Each line uses '... AS colname[,]' form
    """
    # get the SELECT block between SELECT and FROM (first occurrence)
    m = re.search(r"select\s+(.*?)\s+from\s", sql, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        raise ValueError("Unable to locate SELECT ... FROM in SQL.")

    cols_block = m.group(1)

    cols: List[str] = []
    for raw_line in cols_block.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        # match '... AS colname' (case-insensitive, optional trailing comma)
        m_alias = re.search(
            r"\s+AS\s+([A-Za-z_][A-Za-z0-9_]*)\s*,?\s*$",
            line,
            flags=re.IGNORECASE,
        )
        if not m_alias:
            # no alias, skip – we only care about aliased outputs
            continue

        col = m_alias.group(1)
        cols.append(col)

    if not cols:
        raise ValueError("No SELECT aliases (AS colname) found in SQL.")
    return cols


def choose_partition_column(cols: List[str]) -> str:
    """
    Pick partition column with priority:
      to_dt > etl_effective_dt > effectv_dt > last_change_dt
    """
    priority = ["to_dt", "etl_effective_dt", "effectv_dt", "last_change_dt"]
    lower_cols = {c.lower(): c for c in cols}

    for cand in priority:
        if cand in lower_cols:
            return lower_cols[cand]

    # fallback: no partition column found
    return ""


def read_target_table_from_csv(
    csv_path: str,
    entity: str,
    source_col_name: str = 'Table/File Name * (auto populate)',
    target_col_name: str = 'Target Table/File Name * (auto populate)',
) -> str:
    """
    Look up target table name from the original mapping CSV:
      - find rows where source table == entity
      - read distinct non-empty values from 'Target Table/File Name * (auto populate)'
    """
    targets = set()

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        # if exact headers are not present, try a fuzzy fallback
        fieldnames_lower = {fn.lower(): fn for fn in reader.fieldnames or []}

        if source_col_name not in (reader.fieldnames or []):
            # fuzzy match for source col
            for fn_lower, fn in fieldnames_lower.items():
                if "table/file name" in fn_lower and "auto populate" in fn_lower and "target" not in fn_lower:
                    source_col_name = fn
                    break

        if target_col_name not in (reader.fieldnames or []):
            # fuzzy match for target col
            for fn_lower, fn in fieldnames_lower.items():
                if "target table/file name" in fn_lower and "auto populate" in fn_lower:
                    target_col_name = fn
                    break

        for row in reader:
            src_table = (row.get(source_col_name) or "").strip()
            tgt_table = (row.get(target_col_name) or "").strip()
            if not tgt_table:
                continue
            if src_table == entity:
                targets.add(tgt_table)

    if not targets:
        # fall back to using the entity name itself if nothing is found
        return entity

    # If multiple, pick one deterministically (sorted for stability)
    return sorted(targets)[0]


def build_load_enrich_block(
    view_name: str,
    malcode: str,
    target_table: str,
    select_columns: List[str],
    partition_col: str,
) -> str:
    """
    Build the hybrid load_enrich_process block as a text snippet.
    """
    cols_sql = ", ".join(select_columns)
    sql_stmt = f"SELECT {cols_sql} FROM {view_name}"

    target_path = f"${{adls.stage.root}}/${{{malcode}}}".replace("{malcode}", malcode)

    # If no partition column could be found, keep it empty string
    partition_by = f'"{partition_col}"' if partition_col else '""'

    block = f"""load_enrich_process:{{
  Options: {{
    module: load_enrich_process
    method: process
  }}
  loggable: true
  sql: "{sql_stmt}"
  target-path: {target_path}
  mode-of-write: "replace-partition"
  keys: ""
  cdc-flag: false
  scd2-flag: false
  partition-by: {partition_by}
  target-format: delta
  target_table: "/{target_table}"
  name: {target_table}
}}
"""
    return block


def main():
    ap = argparse.ArgumentParser(
        description="Build load_enrich_process block from dt JSON + v9 mapping CSV."
    )
    ap.add_argument(
        "--dt-json",
        required=True,
        help="Path to dt_<malcode>_<entity>.json (data transformation view JSON).",
    )
    ap.add_argument(
        "--csv",
        required=True,
        help="Path to source_target_mapping_clean_v9_fixed(3).csv.",
    )
    ap.add_argument(
        "--out",
        required=False,
        help="Output file path for load_enrich job snippet. If not set, prints to stdout.",
    )

    args = ap.parse_args()

    dt = read_dt_json(args.dt_json)
    view_name, malcode, entity = extract_view_info(dt)

    sql = dt.get("sql") or ""
    if not sql:
        raise ValueError("dt JSON does not contain 'sql' field.")

    select_cols = extract_select_columns(sql)
    partition_col = choose_partition_column(select_cols)
    target_table = read_target_table_from_csv(args.csv, entity)

    block = build_load_enrich_block(
        view_name=view_name,
        malcode=malcode,
        target_table=target_table,
        select_columns=select_cols,
        partition_col=partition_col,
    )

    if args.out:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(block)
        print(f"✅ load_enrich job written to {args.out}")
    else:
        print(block)


if __name__ == "__main__":
    main()
