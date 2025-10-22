# Json Generator

## How to run

### ```bash
### python json-generator/src/build_sql_job.py json-generator/data/
### source_target_mapping_clean_v9_fixed.csv \
###  --outdir json-generator/generated_jobs_full_run_v1 \
###  --source_malcode ND

This project automates the generation of full SQL pipelines, job JSONs, and auditable transformation rule markdowns from source-to-target mapping CSV files.

It converts unstructured free-form **Transformation Rules**, **Business Rules**, and **Join Clauses** into standardized SQL logic with CTE-style transformations.

- Full executable SQL pipeline (`*_pipeline.sql`)
- Structured job JSON (`*_job.json`)
- Human-auditable rule mapping (`transformation_rules_audit.md`)

---

## 📂 Folder Structure

```
json-generator/
│
├── .copilot/
│   └── agents/
│       └── sql_flow_builder.yaml           ← Copilot Agent definition
│
├── src/
│   ├── build_sql_job.py                    ← Main entrypoint script
│   ├── rule_utils.py                       ← Rule extraction and parser logic
│   └── __init__.py
│
├── data/
│   └── source_target_mapping_clean_v9_fixed.csv
│
├── generated_jobs_full_run_v1/             ← Auto-created output folder
│   └── tantrum_job/
│       ├── tantrum_pipeline.sql
│       ├── tantrum_job.json
│       └── transformation_rules_audit.md
│
├── requirements.txt
├── README.md
└── .gitignore
```

---

## 🧰 Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/<org>/json-generator.git
   cd json-generator
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate   # On Windows use: .venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

---

## 🚀 Running the Generator Manually

```bash
python json-generator/src/build_sql_job.py json-generator/data/source_target_mapping_clean_v9_fixed.csv   --outdir json-generator/generated_jobs_full_run_v1   --source_malcode ND
```

All outputs are saved under:
```
json-generator/generated_jobs_full_run_v1/<target_table>_job/
```

---

## 🤖 Using GitHub Copilot Agent (VS Code)

GitHub Copilot in VS Code can automate your SQL job generation interactively using **Agent Mode**.

### 🧩 YAML Definition

Located at:
```
.copilot/agents/sql_flow_builder.yaml
```

This defines an Agent named **“SQL Flow Builder”** that listens for natural-language triggers like:

- `generate sql flow`
- `build job for`
- `create pipeline for`

### 🧠 Example Conversation in VS Code Copilot:

You:  
> build job for `json-generator/data/source_target_mapping_clean_v9_fixed.csv`

Copilot Agent will:
1. Detect the CSV file and infer the target name.  
2. Run the generator (`build_sql_job.py`).  
3. Save the SQL + Job JSON + Audit Markdown under `generated_jobs_full_run_v1/`.  
4. Open the generated SQL pipeline automatically for inspection.  

✅ Example summary shown by Copilot:
```
Generated SQL flow for data/source_target_mapping_clean_v9_fixed.csv
Output Directory: generated_jobs_full_run_v1/tantrum_job/
 - tantrum_pipeline.sql
 - tantrum_job.json
 - transformation_rules_audit.md
```

---

## 🧩 Copilot Agent File (for reference)

```yaml
# .copilot/agents/sql_flow_builder.yaml
name: SQL Flow Builder
description: |
  Automatically generate SQL + Job JSON pipelines from source-to-target mapping CSVs.
  Supports CTE-based transformations, business-rule parsing, and full Copilot integration.

triggers:
  - pattern: "generate sql flow"
  - pattern: "build job for"
  - pattern: "create pipeline for"

steps:
  - ask: |
      Please provide the full CSV file path (e.g., `data/source_target_mapping_clean_v9_fixed.csv`)

  - run: |
      filename=$(basename ${filePath})
      target_name="${filename%.*}"
      echo "Detected target name: ${target_name}"

  - run: |
      python json-generator/src/build_sql_job.py ${filePath} --outdir json-generator/generated_jobs_full_run_v1

  - summarize: |
      ✅ Generated SQL flow for **${filePath}**
      📂 Output: `json-generator/generated_jobs_full_run_v1/${target_name}_job/`
      - SQL: `${target_name}_pipeline.sql`
      - Job JSON: `${target_name}_job.json`
      - Audit: `transformation_rules_audit.md`

  - run: |
      code json-generator/generated_jobs_full_run_v1/${target_name}_job/${target_name}_pipeline.sql

permissions:
  filesystem: read-write
  terminal: allowed
  code_editor: open
```

---

## 🧾 Example Output Files

```
generated_jobs_full_run_v1/
└── tantrum_job/
    ├── tantrum_pipeline.sql
    ├── tantrum_job.json
    └── transformation_rules_audit.md
```

---

## 🧱 Notes

- The generator preserves `FROM ...` clauses and inline comments in CASE logic.
- Business rules are converted into auditable `WHERE` predicates.
- Joins and transformations are converted into clean SQL CTE patterns.
- The framework auto-creates subfolders per target table.
- Existing outputs are overwritten safely on regeneration.

---

## 🧩 Future Enhancements
- Optional validation modules before SQL generation
- CI/CD GitHub Actions integration for auto-generation on CSV updates
- Schema consistency checks across modules

---

**Author:** Data Engineering Automation Team  
**Version:** 1.0.0  
**Last Updated:** 2025-10-22
