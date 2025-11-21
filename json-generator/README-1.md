# JSON Job Generator – SQL / CTE / Job Builder

This module generates end-to-end job definitions (data sourcing, data transformation CTE, and load-enrich job blocks) from a **source–target mapping CSV** using NLP, metadata, and rule interpretation.

It produces:

- A **data_sourcing** module (semi-JSON / HOCON style)
- A **data_transformation** view (`dt_<malcode>_<base_entity>`)
- A **load_enrich_process** block
- A final combined job file:

```text
cw_<job_id>_<target_table>_<malcode>.txt

The flow is orchestrated by:
python json-generator/src/run_full_job_v1.py

One-Shot Pipeline: run_full_job_v1.py

From repo root:

cd design-a-like-app/json-generator

python src/run_full_job_v1.py ^
  --csv "design-a-like-app/json-generator/data/source_target_mapping_clean_v9_fixed (3).csv" ^
  --malcode mcb ^
  --job-id 15000 ^
  --outdir "design-a-like-app/json-generator/generated_out"


(PowerShell with ^ line continuations; backticks ` also work.)

Arguments:

--csv
Full or relative path to the mapping CSV.

--malcode
Source malcode to use in DT view naming: dt_<malcode>_<base_entity>.
Example: mcb, aaw, nd.

--job-id
Used in final combined job file name:
cw_<job-id>_<target_table>_<malcode>.txt.

--outdir
Directory where all intermediate + final artifacts are written.

--scripts-dir (optional)
If not provided, the script uses src/ alongside run_full_job_v1.py.
Override this if you move scripts into a different folder.

What Gets Generated

In generated_out/ (example names):

nlp_rules_interpretation_v6.json

source_columns_summary_v7c.json (if generated separately)

lookup_reference_module_v3.txt

Contains lookup view blocks: lk_mcb_cd, lk_aaw_cd, etc.

data_sourcing_module_v11a.txt

source.malcode, source.basepath, modules { data_sourcing_process { ... } }.

model_v1.json

base_entity

inferred joins

derived columns

static assignments

dt_mcb_ossbr_2_1.json

sql: the full CTE-based SELECT with joins, lookups, WHERE, QUALIFY.

dt_mcb_ossbr_2_1.job.txt

Job wrapper for the DT view.

load_enrich_mcb_ossbr_2_1.job.txt

load_enrich_process block.

cw_15000_tantrum_mcb.txt (example)

Combined data_sourcing + lookup view(s) + dt view + load_enrich in one file.

Lookup Behaviour (v12 multi-lookup)

build_dt_view_from_model_v12_multi_lkp.py:

Scans derived columns in model_v1.json.

For target names ending in _cd WITHOUT static assignment:

Wraps the original expression:

CAST(
  CASE WHEN <lk_alias>.stndrd_cd_name = '<target_cd>'
       THEN <lk_alias>.stndrd_cd_value
       ELSE <original_expression>
  END AS STRING
) AS <target_cd>


Adds a LEFT JOIN after all source joins:

LEFT JOIN lk_mcb_cd lk_mcb_cd_2
  ON mas.srcurrcode = lk_mcb_cd_2.source_value1
 AND lk_mcb_cd_2.stndrd_cd_name = 'curncy_cd'


Supports multiple lookup views per malcode, e.g.:

lk_mcb_cd (for curncy_cd, tantrum_family_cd, etc.)

lk_aaw_cd (for int_freq_cd)

as long as lookup_reference_module_v3.txt defines them.

PowerShell Helper (.ps1)

You can wrap the run command in a simple PowerShell script, e.g.:

# run_job.ps1
param(
  [string]$CsvPath    = "design-a-like-app/json-generator/data/source_target_mapping_clean_v9_fixed (3).csv",
  [string]$Malcode    = "mcb",
  [string]$JobId      = "15000",
  [string]$OutDir     = "design-a-like-app/json-generator/generated_out"
)

python "design-a-like-app/json-generator/src/run_full_job_v1.py" `
  --csv "$CsvPath" `
  --malcode "$Malcode" `
  --job-id "$JobId" `
  --outdir "$OutDir"


Run:

.\run_job.ps1 -Malcode mcb -JobId 15000



---

## 4️⃣ Runbook: `RUNBOOK_JOB_GENERATOR.md`

Place this at:

> `design-a-like-app/json-generator/docs/RUNBOOK_JOB_GENERATOR.md`

```markdown
# Runbook – SQL Job Generator (DT / Load Enrich / Lookup)

## 1. Purpose

This runbook explains how to:

- Generate **data_sourcing**, **data_transformation**, and **load_enrich** job blocks
- Stitch lookup reference views into the DT view
- Produce a final combined job file for a given **malcode** and **job id**

Target audience:

- Data engineers
- BAs or leads validating mappings and joins
- Developers using GitHub Copilot agent mode

---

## 2. Pre-Run Checklist

1. **Python Installed**
   - Python 3.10+ (3.11 recommended)
2. **Dependencies Installed**
   - From `design-a-like-app/json-generator`:

     ```bash
     python -m pip install -r requirements.txt
     ```

3. **Source–Target Mapping CSV Ready**
   - Typically:
     `data/source_target_mapping_clean_v9_fixed (3).csv`
   - Header flattened, fields quoted.

4. **v7c JSON Prepared (if you need manual control)**
   - Run once:

     ```bash
     python src/extract_sources_columns_v7c.py ^
       "data/source_target_mapping_clean_v9_fixed (3).csv" ^
       --nlp_json "generated_out/nlp_rules_interpretation_v6.json" ^
       --outdir "generated_out"
     ```

   - Then **optionally hand-edit**:
     - `source_columns_summary_v7c.json`
       - Fix aliases (`glsxref glsx`, `mfin`, etc.)
       - Fix base entities if needed
       - Adjust join metadata, candidate predicates

---

## 3. Standard End-to-End Run (Single Job)

From repo root:

```bash
cd design-a-like-app/json-generator


3.1 Run the full pipeline
python src/run_full_job_v1.py ^
  --csv "design-a-like-app/json-generator/data/source_target_mapping_clean_v9_fixed (3).csv" ^
  --malcode mcb ^
  --job-id 15000 ^
  --outdir "design-a-like-app/json-generator/generated_out"

Inputs:

--csv : Source–target mapping CSV path

--malcode : Source system (e.g., mcb, aaw, nd)

--job-id : Logical job id (for naming final CW file)

--outdir : Output directory (artifacts)

Outputs (in generated_out/):

nlp_rules_interpretation_v6.json

source_columns_summary_v7c.json (pre-existing, not regenerated in this runner)

lookup_reference_module_v3.txt

data_sourcing_module_v11a.txt

model_v1.json

dt_<malcode>_<base_entity>.json

dt_<malcode>_<base_entity>.job.txt

load_enrich_<malcode>_<base_entity>.job.txt

cw_<job-id>_<target_table>_<malcode>.txt

4. Key Responsibilities (Who Does What)
Data Engineer

Owns run_full_job_v1.py execution

Maintains src/*.py scripts

Oversees lookup_reference_module_v3.py logic

Ensures all generated SQL is syntactically valid

Business Analyst / SME

Validates that:

Derived columns match requirement docs

Lookup rules for _cd columns (e.g., curncy_cd, tantrum_family_cd) match business rules

WHERE / QUALIFY conditions align with acceptance criteria

5. Validating the DT View

Open generated_out/dt_<malcode>_<base>.json

Check:

Base entity in FROM clause is correct

Left joins for sources (e.g., glsxref glsx, mfin, mfspric) match your design

Lookup joins appear after all source joins, for example:

LEFT JOIN lk_mcb_cd lk_mcb_cd_1
  ON mas.srsectype = lk_mcb_cd_1.source_value1
 AND lk_mcb_cd_1.stndrd_cd_name = 'tantrum_family_cd'

LEFT JOIN lk_mcb_cd lk_mcb_cd_2
  ON mas.srcurrcode = lk_mcb_cd_2.source_value1
 AND lk_mcb_cd_2.stndrd_cd_name = 'curncy_cd'

LEFT JOIN lk_aaw_cd lk_aaw_cd_3
  ON tantrum.instr_family_cd = lk_aaw_cd_3.source_value1
 AND lk_aaw_cd_3.stndrd_cd_name = 'int_freq_cd'


3. Make sure static _cd columns with explicit SET TO ... do not have lookups.

6. Validating the Combined Job

Open the final file, e.g.:

generated_out/cw_15000_tantrum_mcb.txt

Verify:

source.malcode blocks look correct (MCB, AAW, etc.)

modules { contains:

data_sourcing_process

All lookup views (lk_mcb_cd, lk_aaw_cd, ...)

dt_<malcode>_<base_entity>

load_enrich_process

Path placeholders (${adls.source.root}, ${adls.lookup.path}, ${adls.stage.root}) are intact.

7. Troubleshooting
7.1 FileNotFoundError: source_columns_summary_v7c.json

Cause: run_full_job_v1.py expects v7c to already exist.

Fix:
python src/extract_sources_columns_v7c.py ^
  "data/source_target_mapping_clean_v9_fixed (3).csv" ^
  --nlp_json "generated_out/nlp_rules_interpretation_v6.json" ^
  --outdir "generated_out"

Then manually adjust source_columns_summary_v7c.json if needed and rerun run_full_job_v1.py.

7.2 Joins Look Different Between “Single Script” vs “Full Run”

Confirm that both are pointing to the same:

source_columns_summary_v7c.json

build_dt_view_from_model_v12_multi_lkp.py (path + timestamp)

If multiple copies exist, standardize on the version in src/.

7.3 Lookup Joins Missing or Extra

For missing lookup join:

Ensure target name ends with _cd

Ensure it does not have a static assignment in model_v1.json

For extra lookup join (e.g., mu_fund_family_cd when you want the pure CASE):

Either:

adjust the script to skip that specific target, or

mark it as static in the metadata so the lookup wrapper is not applied.

8. Frequently Used Commands
Regenerate Only DT View (after adjusting v7c)
python src/build_model_v1_v11c.py ^
  --input "generated_out/source_columns_summary_v7c.json" ^
  --output "generated_out/model_v1.json"

python src/build_dt_view_from_model_v12_multi_lkp.py ^
  --model "generated_out/model_v1.json" ^
  --v7c   "generated_out/source_columns_summary_v7c.json" ^
  --malcode mcb ^
  --output "generated_out/dt_mcb_ossbr_2_1.json"


Rebuild Only Combined Job (after manual edits to DT or lookup)
python src/build_combined_job_v5.py ^
  --data-sourcing "generated_out/data_sourcing_module_v11a.txt" ^
  --lookup        "generated_out/lookup_reference_module_v3.txt" ^
  --dt-view       "generated_out/dt_mcb_ossbr_2_1.job.txt" ^
  --load-enrich   "generated_out/load_enrich_mcb_ossbr_2_1.job.txt" ^
  --job-id        15000 ^
  --output-dir    "generated_out"

