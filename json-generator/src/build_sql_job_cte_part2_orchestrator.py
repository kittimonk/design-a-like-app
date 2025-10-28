#!/usr/bin/env python3
"""
CTE-Based SQL Generator v2 — Part 2 (Orchestrator)
--------------------------------------------------
- Imports Part 1 core engine to parse CSV and synthesize per-source and final SQL.
- Writes:
  * dt_<target>_<malcode>_<SOURCE>.sql   (per-source)
  * dt_<target>_<malcode>.sql            (final integrated view)
  * ew_123_<target>_<malcode>.json       (flattened modules, as requested)
  * transformation_<target>_<malcode>_rules_audit.md
  * debug logs in debug_outputs/
"""

from __future__ import annotations
import os, re, json, argparse
from typing import Dict, List, Tuple
from pathlib import Path
from collections import Counter

# ---- Import Part 1 core engine public APIs ----
from build_sql_job_cte_part1_core import (
    parse_mapping, build_per_source_ctes, build_final_select_only, ParsedMapping
)

DEBUG_DIR = Path("debug_outputs")
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

def _write_debug(name: str, content: str) -> None:
    p = DEBUG_DIR / name
    with p.open("a", encoding="utf-8") as f:
        f.write(content.rstrip() + "\n\n")

def _safe(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", (name or "").strip())

# ------------------------
# SQL Validation Utilities
# ------------------------

def validate_sql(sql_text: str) -> List[str]:
    """
    Lightweight validator to detect obvious SQL issues before writing output.
    Returns a list of error strings.
    """
    errors = []
    # 1) Duplicate alias usage
    aliases = re.findall(r"\bJOIN\s+[A-Za-z0-9_\.]+\s+([A-Za-z0-9_]+)\b", sql_text, flags=re.I)
    dupes = [a for a, c in Counter(aliases).items() if c > 1]
    if dupes:
        errors.append(f"Duplicate aliases found: {', '.join(dupes)}")

    # 2) JOINs missing ON clause
    join_lines = re.findall(r"LEFT\s+JOIN\s+[A-Za-z0-9_\.]+\s+[A-Za-z0-9_]+(?![^\\n]*\\bON\\b)", sql_text, flags=re.I)
    if join_lines:
        errors.append(f"JOINs missing ON clause detected: {len(join_lines)} potential issues")

    # 3) Unbalanced CASE/END
    if sql_text.upper().count("CASE") != sql_text.upper().count("END"):
        errors.append("Unbalanced CASE/END blocks")

    # 4) Parentheses mismatch
    if sql_text.count("(") != sql_text.count(")"):
        errors.append("Mismatched parentheses count")

    return errors

# ------------------------
# JSON Builders
# ------------------------

def build_data_sourcing_module(sources: List[str]) -> Dict:
    mod = {
        "options": {"module": "data_sourcing_process", "method": "process"},
        "loggable": True,
        "sourcelist": sources,
    }
    for s in sources:
        mod[s] = {
            "type": "sz_zone",
            "table.name": s,
            "read-format": "view",
            "path": "${adls.source.root}/" + s
        }
    return mod

def build_flattened_modules(pm: ParsedMapping, out_job_dir: Path, source_malcode: str,
                            per_source_sql: Dict[str,str], driving_source: str) -> Dict:
    """
    Flattened modules: siblings for data_sourcing_process, dt_<target>_<malcode>_<SRC>..., dt_<target>_<malcode>, load_enrich_process.
    """
    modules = {}

    # 1) Data Sourcing (as-is)
    modules["data_sourcing_process"] = build_data_sourcing_module(pm.sources)

    # 2) Per-source dt_* modules with inline SQL
    target = _safe(pm.target_table).lower()
    for s in pm.sources:
        entry_name = f"dt_{target}_{source_malcode.lower()}_{_safe(s)}"
        sql_file = out_job_dir / f"{entry_name}.sql"
        sql_text = ""
        if sql_file.exists():
            sql_text = sql_file.read_text(encoding="utf-8").strip()
        modules[entry_name] = {
            # Embed SQL inline as a literal triple-quoted string
            "sql": f'"""{sql_text}"""',
            "loggable": True,
            "options": {"module": "data_transformation", "method": "process"},
            "name": entry_name
        }

    # 3) Final dt_<target>_<malcode> with inline SQL
    final_name = f"dt_{target}_{source_malcode.lower()}"
    final_sql_file = out_job_dir / f"{final_name}.sql"
    final_sql_text = ""
    if final_sql_file.exists():
        final_sql_text = final_sql_file.read_text(encoding="utf-8").strip()
    modules[final_name] = {
        "sql": f'"""{final_sql_text}"""',
        "loggable": True,
        "options": {"module": "data_transformation", "method": "process"},
        "name": final_name
    }

    # # 2) Per-source dt_* modules
    # target = _safe(pm.target_table).lower()
    # for s in pm.sources:
    #     entry_name = f"dt_{target}_{source_malcode.lower()}_{_safe(s)}"
    #     sql_rel = out_job_dir / f"{entry_name}.sql"
    #     modules[entry_name] = {
    #         "sql": f"@{sql_rel.as_posix()}",
    #         "loggable": True,
    #         "options": {"module": "data_transformation", "method": "process"},
    #         "name": entry_name
    #     }

    # # 3) Final dt_<target>_<malcode>
    # final_name = f"dt_{target}_{source_malcode.lower()}"
    # final_sql_rel = out_job_dir / f"{final_name}.sql"
    # modules[final_name] = {
    #     "sql": f"@{final_sql_rel.as_posix()}",
    #     "loggable": True,
    #     "options": {"module": "data_transformation", "method": "process"},
    #     "name": final_name
    # }

    # 4) Load Enrich (consumes final view)
    modules["load_enrich_process"] = {
        "options": {"module": "load_enrich_process", "method": "process"},
        "loggable": True,
        "sql": f"SELECT * FROM {final_name}",
        "target-path": f"${{adls.stage.root}}/{source_malcode}",
        "mode-of-write": "replace_partition",
        "keys": "",
        "cdc-flag": False,
        "scd2-flag": False,
        "partition-by": "effective_dt",
        "target-format": "delta",
        "target-table": f"/{pm.target_table}",
        "name": f"{pm.target_table}_daily"
    }
    return modules

# ------------------------
# File Writers
# ------------------------

def write_per_source_sqls(pm: ParsedMapping, per_source_sql: Dict[str,str], out_job_dir: Path, source_malcode: str) -> Dict[str, Path]:
    """
    Writes per-source SQL files and returns mapping of source -> file path.
    """
    paths = {}
    tgt_safe = _safe(pm.target_table).lower()
    for s, sql in per_source_sql.items():
        name = f"dt_{tgt_safe}_{source_malcode.lower()}_{_safe(s)}.sql"
        p = out_job_dir / name
        p.write_text(sql.rstrip() + "\n", encoding="utf-8")
        paths[s] = p
        # Validate
        errs = validate_sql(sql)
        if errs:
            _write_debug("sql_validator.log", f"[{name}] issues:\n- " + "\n- ".join(errs))
    return paths

def write_final_sql(pm: ParsedMapping, select_sql: str, out_job_dir: Path, source_malcode: str,
                    driving_source: str) -> Path:
    """
    Composes the final SQL selecting columns from the driving per-source view,
    with placeholders for joining other per-source views.
    """
    tgt_safe = _safe(pm.target_table).lower()
    final_name = f"dt_{tgt_safe}_{source_malcode.lower()}"
    driving_view = f"dt_{tgt_safe}_{source_malcode.lower()}_{_safe(driving_source)}"

    # Build JOIN placeholders for other views (commented)
    others = [s for s in pm.sources if s.lower() != driving_source.lower()]
    join_lines = []
    for s in others:
        view = f"dt_{tgt_safe}_{source_malcode.lower()}_{_safe(s)}"
        join_lines.append(f"-- LEFT JOIN {view} v_{_safe(s)} ON 1=1  -- TODO: replace with real keys")

    final_sql = f"""{select_sql}
FROM {driving_view} base
{os.linesep.join(join_lines)};
"""
    p = out_job_dir / f"{final_name}.sql"
    p.write_text(final_sql, encoding="utf-8")

    # Validate
    errs = validate_sql(final_sql)
    if errs:
        _write_debug("sql_validator.log", f"[{final_name}.sql] issues:\n- " + "\n- ".join(errs))

    return p

def write_audit_md(audit_rows: List[Dict[str,str]], out_job_dir: Path, source_malcode: str, target_table: str) -> Path:
    md = out_job_dir / f"transformation_{_safe(target_table).lower()}_{source_malcode.lower()}_rules_audit.md"
    with md.open("w", encoding="utf-8") as f:
        f.write("# Transformation Rules Audit\n\n")
        f.write("| Target Column | Raw Transformation (verbatim) | Parsed SQL Expression | Notes |\n")
        f.write("|---|---|---|---|\n")
        for r in audit_rows:
            raw = (r.get("raw","") or "").replace("\n", "<br>").replace("|", "\\|")
            sql = (r.get("sql","") or "").replace("\n", "<br>").replace("|", "\\|")
            note = (r.get("note","") or "").replace("\n", "<br>").replace("|", "\\|")
            f.write(f"| `{r.get('target','')}` | {raw} | `{sql}` | {note} |\n")
    return md

def write_flattened_json(pm: ParsedMapping, out_job_dir: Path, source_malcode: str, modules: Dict) -> Path:
    job_json = {
        "source malcode": source_malcode,
        "source basepath": source_malcode.upper(),
        "comment": f"This job is responsible for loading data into {pm.target_table} from {source_malcode} - " +
                   ", ".join(pm.sources),
        "modules": modules
    }
    p = out_job_dir / f"ew_123_{_safe(pm.target_table).lower()}_{source_malcode.lower()}.json"
    p.write_text(json.dumps(job_json, indent=2), encoding="utf-8")
    return p

# ------------------------
# Main orchestration
# ------------------------

def generate(csv_path: str, outdir: str, source_malcode: str = "ND", driving_source: str | None = None) -> Dict[str, str]:
    pm = parse_mapping(csv_path)
    out_dir = Path(outdir)
    out_dir.mkdir(parents=True, exist_ok=True)
    job_dir = out_dir / f"{_safe(pm.target_table).lower()}_job"
    job_dir.mkdir(parents=True, exist_ok=True)

    # Per-source CTE SQLs
    per_source_sql = build_per_source_ctes(pm, base_alias="mas")
    paths_map = write_per_source_sqls(pm, per_source_sql, job_dir, source_malcode)

    # Final SELECT (column list) + audit
    select_sql, audit_rows = build_final_select_only(pm, driving_source=driving_source)

    # Final integrated SQL based on driving per-source view
    driving = driving_source or (pm.sources[0] if pm.sources else "source")
    final_sql_path = write_final_sql(pm, select_sql, job_dir, source_malcode, driving)

    # JSON modules (flattened) + write
    modules = build_flattened_modules(pm, job_dir, source_malcode, per_source_sql, driving)
    json_path = write_flattened_json(pm, job_dir, source_malcode, modules)

    # Audit markdown
    md_path = write_audit_md(audit_rows, job_dir, source_malcode, pm.target_table)

    # Return artifact paths
    return {
        "target": pm.target_table,
        "out_job_dir": str(job_dir),
        "final_sql": str(final_sql_path),
        "json_path": str(json_path),
        "audit_md": str(md_path),
        **{f"per_source_sql::{k}": str(v) for k,v in paths_map.items()}
    }

def _cli():
    ap = argparse.ArgumentParser(description="CTE-based SQL Generator v2 — Part 2 Orchestrator")
    ap.add_argument("csv", help="Path to mapping CSV")
    ap.add_argument("--outdir", default="generated_jobs_full_run_v2", help="Output directory")
    ap.add_argument("--source_malcode", default="ND", help="Source MAL code (e.g., ND)")
    ap.add_argument("--driving_source", default=None, help="Driving source for final view (defaults to first source found)")
    args = ap.parse_args()
    res = generate(args.csv, args.outdir, args.source_malcode, args.driving_source)
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    _cli()
