# SQL Job Generator

This tool converts a Source-to-Target Mapping CSV (with free-form Business Rules,
Join Clauses, and Transformation Logic) into:

- Full executable SQL pipeline (`*_pipeline.sql`)
- Structured job JSON (`*_job.json`)
- Human-auditable rule mapping (`transformation_rules_audit.md`)

## How to run

```bash
python src/build_sql_job.py data/source_target_mapping_clean_v9_fixed.csv \
  --outdir generated_jobs_full_run_v1 \
  --source_malcode ND
