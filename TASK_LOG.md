# Task log

## 2026-10-27: Job specs match the actual Git folder paths

- **Task:** the qa/prod Git folders were created as `orders-pipelines_qa` / `orders-pipelines_prod`; align the job specs and README.
- **Files changed:** `jobs/qa/*.json`, `jobs/prod/*.json`, `README.md`.
- **Summary:** notebook paths now point at `/Workspace/Shared/orders-pipelines_{qa,prod}`. The 9 workspace jobs were updated in place (same job IDs) to these specs.
- **Status:** done; the deployed jobs already match.

## 2026-10-27: Initial repo (split from the pipeline agent repo)

- **Task:** move the sample pipelines into their own repo as pure-PySpark Databricks notebooks; tests run inside Databricks.
- **Files changed:** `notebooks/*.py` (5 pipelines), `tests/test_*.py` (5 test notebooks), `setup/00_setup_demo.py`, `setup/10_seed_scenario.py`, `setup/99_reset_demo.py`, `jobs/{dev,qa,prod}/*.json`, `faults/*.yaml`, `.github/workflows/ci.yml`, `.github/CODEOWNERS`, `README.md`.
- **Summary:** same behaviour as the former `sample_pipelines/src` scripts (schemas, MERGE key, error codes), now as notebook cells with widgets instead of argparse and a `common/` package. Data seeding moved from laptop scripts into setup notebooks. CI lints notebooks; Spark tests run in Databricks.
- **Status:** done locally (compile + pyflakes pass). Not yet run in Databricks; pending GitHub repo creation, Git folders and jobs (README "Setup").
