# GitHub Copilot Instructions for `design-a-like-app`

## Overview
This repository automates the generation of SQL pipelines, job JSONs, and auditable transformation rule markdowns from source-to-target mapping CSV files. It uses Python scripts to parse transformation rules, business logic, and join clauses into structured SQL logic.

Key components include:
- **`build_sql_job.py`**: Main script for generating SQL pipelines.
- **`rule_utils.py`**: Utility functions for parsing and extracting rules.
- **Generated Outputs**:
  - SQL pipeline (`*_pipeline.sql`)
  - Job JSON (`*_job.json`)
  - Transformation audit markdown (`transformation_rules_audit.md`)

## Developer Workflows

### Running the Generator
To manually generate SQL flows:
```bash
python json-generator/src/build_sql_job.py json-generator/data/source_target_mapping_clean_v9_fixed.csv \
  --outdir json-generator/generated_jobs_full_run_v1 \
  --source_malcode ND
```
Outputs are saved in `json-generator/generated_jobs_full_run_v1/<target_table>_job/`.

### Using the Copilot Agent
The repository includes a Copilot Agent defined in `.copilot/agents/sql_flow_builder.yaml`. This agent automates SQL flow generation based on natural language commands.

#### Example Commands:
- `generate sql flow`
- `build job for <CSV file>`
- `create pipeline for <CSV file>`

#### Agent Workflow:
1. Prompts for the CSV file path.
2. Infers the target table name from the file.
3. Runs `build_sql_job.py` to generate outputs.
4. Opens the generated SQL pipeline for review.

### Folder Structure
- **`src/`**: Contains core scripts.
- **`data/`**: Input CSV files.
- **`generated_jobs_full_run_v1/`**: Output directory for generated files.

## Project-Specific Conventions
- **CTE-Based SQL**: All transformations are structured as Common Table Expressions (CTEs).
- **Auditable Rules**: Business rules are converted into `WHERE` predicates with inline comments.
- **Safe Overwrites**: Existing outputs are safely replaced during regeneration.

## Integration Points
- **Python Environment**: Ensure dependencies are installed via `pip install -r requirements.txt`.
- **Copilot Permissions**: The agent requires read-write access to the filesystem and terminal execution.

## Example Outputs
Generated files include:
- `<target_table>_pipeline.sql`
- `<target_table>_job.json`
- `transformation_rules_audit.md`

## Future Enhancements
- Add validation modules for schema consistency.
- Automate SQL generation via CI/CD pipelines.
- Integrate schema checks for input CSVs.

---
For more details, refer to the [README](../json-generator/README.md).