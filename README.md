# orders-pipelines

Sample Databricks pipelines (synthetic orders data) monitored by the **pipeline agent**
(`karthik-np-ust/databricks_agent_repo`). Every pipeline is a Databricks notebook written as
plain top-level PySpark cells: no wrapper functions, no shared Python package.

| Path | What |
|---|---|
| `notebooks/` | the pipelines: `ingest_bronze` → `transform_silver` → `build_gold`, `sales_report`, `partner_feed` |
| `tests/` | Databricks test notebooks (isolated `test_<id>_*` schemas, `dbutils.notebook.run`, asserts). The agent's Testing agent adds more with every fix |
| `setup/` | `00_setup_demo` (catalogs, schemas, volume, secret scopes, clean data), `10_seed_scenario` (fault datasets), `99_reset_demo` |
| `jobs/<env>/` | Jobs API 2.2 job specs for dev, qa and prod |
| `faults/` | release faults applied by the agent repo's `scripts/inject_fault.py` |

## Branches and environments

`dev` → `qa` → `main` (= prod). Each branch is checked out in its own Databricks Git folder
and the jobs of that environment run the notebooks from it:

| Branch | Git folder | Jobs | Catalog |
|---|---|---|---|
| `dev` | `/Workspace/Shared/orders-pipelines-dev` | `[dev] orders_pipeline`, … | `dev` |
| `qa` | `/Workspace/Shared/orders-pipelines_qa` | `[qa] orders_pipeline`, … | `qa` |
| `main` | `/Workspace/Shared/orders-pipelines_prod` | `orders_pipeline`, … | `prod` |

Changes reach a branch only by PR. CI (`notebook-lint`, `promotion-path`, `path-guard`) runs on
every PR; branch protection on `qa` and `main` requires Code Owner review.

## Setup

1. **GitHub.** Create the repo (public, so branch protection is free), push `main`, then create
   `dev` and `qa` from it:
   ```bash
   git remote add origin https://github.com/karthik-np-ust/orders-pipelines.git
   git push -u origin main && git push origin main:dev main:qa
   ```
   Protect `dev`, `qa`, `main` (require PR + `notebook-lint`; `qa`/`main` also Code Owner review).
   Install the pipeline agent's GitHub App on this repo and set the repo variable
   `AGENT_BOT_LOGIN` to the app's bot login.
2. **Databricks: link GitHub.** User settings → Linked accounts → GitHub.
3. **Databricks: Git folders.** Workspace → Shared → Create → Git folder, URL of this repo,
   name `orders-pipelines-dev`; switch it to branch `dev`. Repeat for `orders-pipelines_qa`
   (`qa`) and `orders-pipelines_prod` (`main`).
4. **Demo data.** Open `orders-pipelines-dev/setup/00_setup_demo` and run all
   (creates `dev`, `qa`, `prod` catalogs, schemas, the landing volume, secret scopes and clean data).
   If `CREATE CATALOG` is not allowed, run it with `layout = schema` and use
   `catalog = workspace`, `layout = schema` in the job parameters.
5. **Jobs.** Create the 9 jobs from `jobs/<env>/*.json`, either in the UI (Jobs & Pipelines →
   Create job, add notebook tasks with the paths and parameters from the JSON) or with the API:
   ```bash
   curl -sS -X POST "$DATABRICKS_HOST/api/2.2/jobs/create" \
        -H "Authorization: Bearer $DATABRICKS_TOKEN" -d @jobs/prod/orders_pipeline.json
   ```
   Tasks run on serverless compute (no cluster settings). Job parameters reach the notebooks as widgets.
6. **Smoke test.** Run `[dev] orders_pipeline`, then each `tests/test_*` notebook in the dev Git folder.

## Fault scenarios

| # | How | Expected |
|---|---|---|
| 1 | run `sales_report` with `sleep_seconds = 900` | slow run (threshold 300s) |
| 2 | run `partner_feed_ingest` with `flaky_rate = 0.7` | transient `HTTP 503` |
| 3–7, 12 | `setup/10_seed_scenario` (`env = prod`, `scenario = …`), then run `orders_pipeline` | see the notebook's table |
| 8–10 | `python scripts/inject_fault.py <name> --repo-path ../orders-pipelines --push` (agent repo) | `UNRESOLVED_COLUMN`, `TABLE_OR_VIEW_NOT_FOUND`, `SyntaxError` |
| 11 | delete secret `partner-prod/api_key`, run `partner_feed_ingest` | `Secret does not exist` |
| 13 | run `partner_feed_ingest` with `leak_fake_token = true` | fails printing a FAKE token (redaction check) |

`setup/99_reset_demo` puts the data back after each scenario.

## Writing pipeline code

- Top-level PySpark cells; read parameters with `dbutils.widgets`; resolve table names inline.
- Define `spark` (`SparkSession.builder.getOrCreate()`) and import `dbutils` from
  `databricks.sdk.runtime` in the first cell, so `pyflakes` sees no undefined names.
- Never remove or weaken data-quality checks (`DQAssertionError`).
- Every change ships with a test notebook in `tests/` that reproduces the problem in an
  isolated `test_<id>_*` schema and drops it afterwards.
