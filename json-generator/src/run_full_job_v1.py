#!/usr/bin/env python3
"""
run_full_job_v1.py

One-shot pipeline runner for your SQL job generation flow:

Steps:
  1) nlp_rules_parser_v6.py         → nlp_rules_interpretation_v6.json
  2) extract_sources_columns_v7c.py → source_columns_summary_v7c.json
  3) lookup_reference_module_v3.py  → lookup_reference_module_v3.txt  (not yet stitched, but generated)
  4) data_sourcing_cte_v11a.py      → data_sourcing_module_v11a.txt
  5) build_model_v1_v11c.py         → model_v1.json
  6) build_dt_view_from_model_v12_multi_lkp.py → dt_<malcode>_<base>.json
  7) build_job_json_v1.py           → dt_<malcode>_<base>.job.txt
  8) build_load_enrich_from_dt_v2.py → load_enrich_<malcode>_<base>.job.txt
  9) build_combined_job_v4.py       → cw_<job_id>_<target_table>_<malcode>.txt

Everything is parameterized by:
  - CSV path
  - malcode (e.g. mcb, aaw)
  - job-id
  - outdir
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def run(cmd, cwd=None):
    """
    Small helper that prints the command and runs it with check=True.
    """
    #print(f"\n▶ Running: {' '.join(cmd)}")
    print("Running:", " ".join(cmd))
    subprocess.run(cmd, check=True, cwd=cwd)


def main():
    ap = argparse.ArgumentParser(
        description="End-to-end job generator (NLP → v7c → model → dt → load_enrich → combined job)."
    )
    ap.add_argument(
        "--csv",
        required=True,
        help="Path to source_target_mapping_clean_v9_fixed (3).csv",
    )
    ap.add_argument(
        "--malcode",
        required=True,
        help="Source malcode, e.g. mcb, aaw, nd (used in dt_<malcode>_<base>).",
    )
    ap.add_argument(
        "--job-id",
        required=True,
        help="Job id to embed in final combined filename, e.g. 11000.",
    )
    ap.add_argument(
        "--outdir",
        required=True,
        help="Output directory where all intermediate/final artifacts will be written.",
    )
    ap.add_argument(
        "--scripts-dir",
        default=None,
        help=(
            "Directory where the generator scripts live. "
            "If not provided, defaults to the directory containing this file."
        ),
    )
    args = ap.parse_args()

    # Resolve paths
    csv_path = Path(args.csv).resolve()
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    if args.scripts_dir:
        scripts_dir = Path(args.scripts_dir).resolve()
    else:
        scripts_dir = Path(__file__).resolve().parent

    # Script paths (all relative to scripts_dir)
    nlp_parser        = scripts_dir / "nlp_rules_parser_v6.py"
    #extract_v7c       = scripts_dir / "extract_sources_columns_v7c.py"
    lookup_ref_v3     = scripts_dir / "lookup_reference_module_v3.py"
    data_sourcing_v11 = scripts_dir / "data_sourcing_cte_v11a.py"
    model_v11c        = scripts_dir / "build_model_v1_v11c.py"
    dt_view_v11c      = scripts_dir / "build_dt_view_from_model_v12_multi_lkp.py"
    build_job_v1      = scripts_dir / "build_job_json_v1.py"
    load_enrich_v2    = scripts_dir / "build_load_enrich_from_dt_v2.py"
    combined_v4       = scripts_dir / "build_combined_job_v5.py"

    # Output file paths (all under outdir)
    nlp_json          = outdir / "nlp_rules_interpretation_v6.json"
    v7c_json          = outdir / "source_columns_summary_v7c.json"
    v7c_md            = outdir / "source_columns_summary_v7c.md"  # not used, but generated
    lookup_out        = outdir / "lookup_reference_module_v3.txt"
    data_sourcing_out = outdir / "data_sourcing_module_v11a.txt"
    model_json        = outdir / "model_v1.json"

    # 1) NLP rules parser
    #    python nlp_rules_parser_v6.py CSV --outdir OUTDIR
    run(
        [
            sys.executable,
            str(nlp_parser),
            str(csv_path),
            "--outdir",
            str(outdir),
        ]
    )
    # Now nlp_json & nlp_rules_interpretation_v6.md exist in outdir

    # 2) Extract sources/columns v7c
    #    python extract_sources_columns_v7c.py CSV --nlp_json nlp_json --outdir OUTDIR
    #run(
    #    [
    #        sys.executable,
    #        str(extract_v7c),
    #        str(csv_path),
    #        "--nlp_json",
    #        str(nlp_json),
    #        "--outdir",
    #        str(outdir),
    #    ]
    #)
    # v7c_json and v7c_md now exist

    # 3) Lookup reference module v3 (not stitched yet, but generated)
    #    python lookup_reference_module_v3.py --csv CSV --v7c v7c_json --out lookup_out
    run(
        [
            sys.executable,
            str(lookup_ref_v3),
            "--csv",
            str(csv_path),
            "--v7c",
            str(v7c_json),
            "--out",
            str(lookup_out),
        ]
    )

    # 4) Data sourcing CTE v11a
    #    python data_sourcing_cte_v11a.py CSV --out data_sourcing_module_v11a.txt
    run(
        [
            sys.executable,
            str(data_sourcing_v11),
            str(csv_path),
            "--out",
            str(data_sourcing_out),
        ]
    )

    # 5) Build model_v1.json from v7c
    #    python build_model_v1_v11c.py --input v7c_json --output model_json
    run(
        [
            sys.executable,
            str(model_v11c),
            "--input",
            str(v7c_json),
            "--output",
            str(model_json),
        ]
    )

    # Read base_entity from model_v1.json (no hardcoding of ossbr_2_1)
    with model_json.open("r", encoding="utf-8") as f:
        model_obj = json.load(f)
    base_entity = model_obj.get("base_entity")
    if not base_entity:
        raise RuntimeError("model_v1.json missing 'base_entity' – cannot proceed.")

    base_entity_lc = base_entity.lower()
    malcode_lc = args.malcode.lower()

    # 6) Build dt_<malcode>_<base>.json
    dt_json = outdir / f"dt_{malcode_lc}_{base_entity_lc}.json"
    run(
        [
            sys.executable,
            str(dt_view_v11c),
            "--model",
            str(model_json),
            "--v7c",
            str(v7c_json),
            "--malcode",
            args.malcode,
            "--output",
            str(dt_json),
        ]
    )

    # 7) Build dt_<malcode>_<base>.job.txt (job block)
    dt_job_txt = outdir / f"dt_{malcode_lc}_{base_entity_lc}.job.txt"
    run(
        [
            sys.executable,
            str(build_job_v1),
            "--model",
            str(model_json),
            "--sqljson",
            str(dt_json),
            "--malcode",
            args.malcode,
            "--output",
            str(dt_job_txt),
        ]
    )

    # 8) Build load_enrich_<malcode>_<base>.job.txt
    load_enrich_txt = outdir / f"load_enrich_{malcode_lc}_{base_entity_lc}.job.txt"
    run(
        [
            sys.executable,
            str(load_enrich_v2),
            "--dt-json",
            str(dt_json),
            "--csv",
            str(csv_path),
            "--out",
            str(load_enrich_txt),
        ]
    )

    # 9) Combine into final cw_<job_id>_<target_table>_<malcode>.txt
    #    python build_combined_job_v4.py --data-sourcing data_sourcing_out
    #                                    --dt-view dt_job_txt
    #                                    --load-enrich load_enrich_txt
    #                                    --job-id JOB_ID
    #                                    --output-dir OUTDIR
    run(
        [
            sys.executable,
            str(combined_v4),
            "--data-sourcing",
            str(data_sourcing_out),
            "--lookup", 
            str(lookup_out),          # <-- REQUIRED (Fix)
            "--dt-view",
            str(dt_job_txt),
            "--load-enrich",
            str(load_enrich_txt),
            "--job-id",
            args.job_id,
            "--output-dir",
            str(outdir),
        ]
    )

    print("\n✅ End-to-end pipeline complete.")
    print(f"   CSV           : {csv_path}")
    print(f"   malcode       : {args.malcode}")
    print(f"   base_entity   : {base_entity}")
    print(f"   outdir        : {outdir}")
    print("   Key artifacts :")
    print(f"     - {nlp_json.name}")
    print(f"     - {v7c_json.name}")
    print(f"     - {model_json.name}")
    print(f"     - {dt_json.name}")
    print(f"     - {dt_job_txt.name}")
    print(f"     - {load_enrich_txt.name}")
    print("   Final combined job is in OUTDIR as:")
    print("     cw_<job-id>_<target_table>_<malcode>.txt (printed by build_combined_job_v4.py)")


if __name__ == "__main__":
    main()
