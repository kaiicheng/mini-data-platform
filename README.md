# Mini Data Platform

If you have an applied AI interview at Astronomer, we'll ask you to build a small project around this repo. You can also proactively do this as part of your application to speed up the process.

This repo is a synthetic data platform containing mock data csv files, Airflow DAGs, dbt models, Evidence dashboards, and a DuckDB data warehouse. Your objective is to create an agent exposed via a CLI to interact with the data platform. This CLI agent should be geared specifically towards ad-hoc questions and analysis. Things like:

- How much in sales did we do last quarter?
- Which two products are most frequently bought together?
- Are there any anomalies with how we sell products?
- What's our average customer lifetime value?
- ... and other, more complex things!

To complete this, fork the repo and build a CLI agent where you can send questions like the ones above. We have no particular requirements around languages, model providers, methods, etc - instead, we want you to demonstrate how you think about these problems! While this repo is representative of an e-commerce company's data platform, you should aim to keep your implementation generic enough that you could plug in other "mini data platforms". See how much you can infer based on the code and warehouse metadata instead of providing explicit documentation about this data platform to the agent upfront.

To submit, send us a fork of your repo. You should modify / create a new README that outlines your approach and where you'd continue building things if you had more time. This should take no more than a few hours.

## Quick Setup

Run the setup script to initialize everything:

```bash
./setup.sh
```

This will:
1. Generate synthetic data
2. Initialize Airflow and load data into DuckDB
3. Run dbt transformations

Then view the dashboards:

```bash
cd evidence
npm install       # First time only
npm run sources   # Build data sources
npm run dev       # Start dev server
# Open http://localhost:3000
```

---

## Manual Setup (Advanced)

<details>
<summary>Click to expand manual setup steps</summary>

### 1. Install dependencies

```bash
uv sync
```

### 2. Generate synthetic data

```bash
uv run python scripts/generate_all.py
```

### 3. Initialize Airflow

First, update `airflow/airflow.cfg` to use an absolute path for the database:

```bash
cd airflow
# Update sql_alchemy_conn in airflow.cfg to:
# sql_alchemy_conn = sqlite:////absolute/path/to/your/mini-data-platform/airflow/airflow.db

export AIRFLOW_HOME=$(pwd)
uv run airflow db migrate
```

### 4. Run ingestion DAGs

```bash
# From airflow/ directory
export AIRFLOW_HOME=$(pwd)
uv run python dags/ingest_products.py
uv run python dags/ingest_users.py
uv run python dags/ingest_transactions.py
uv run python dags/ingest_campaigns.py
uv run python dags/ingest_pageviews.py
```

### 5. Run dbt transformations

```bash
# From airflow/ directory
export AIRFLOW_HOME=$(pwd)
uv run python dags/run_dbt.py

# Or run dbt directly
cd ../dbt_project
uv run dbt build --profiles-dir .
```

</details>

## Project Structure

```sh
mini-data-platform/
├── sources/              # Raw source data (CSV files)
│   ├── postgres/         # Sales, products, users
│   ├── salesforce/       # Marketing campaigns
│   └── analytics/        # Page view events
├── airflow/
│   ├── dags/            # Airflow DAGs for ingestion and transformation
│   │   ├── ingest_*.py  # Load data from sources → raw schema
│   │   ├── run_dbt.py   # Run dbt staging → marts pipeline
│   │   └── build_evidence.py  # Build Evidence dashboards
│   └── utils/           # Shared utilities
├── warehouse/           # DuckDB database (data.duckdb)
├── dbt_project/         # dbt transformations
│   └── models/
│       ├── staging/     # Clean raw data (5 models)
│       └── marts/       # Analytics-ready tables (3 models)
├── evidence/            # Evidence BI dashboards
│   ├── pages/           # Dashboard pages (index, sales, products, customers)
│   └── sources/         # SQL queries and connection
└── scripts/             # Data generation scripts
```

## Data Pipeline

### Raw Layer (`raw` schema)

- Loaded by Airflow ingestion DAGs
- 5 tables: products, users, transactions, campaigns, pageviews

### Staging Layer (`staging` schema)

- Created by dbt
- 5 views: stg_products, stg_users, stg_transactions, stg_campaigns, stg_pageviews

### Marts Layer (`marts` schema)

- Created by dbt
- Denormalized tables for analysis
- 3 tables:
  - `dim_products`: Current product catalog (62 products)
  - `dim_customers`: Current customer info (5,000 customers)
  - `fct_orders`: Order line items with dimensions (35,980 rows)

## Data Volumes

- **Raw**: ~93K total rows across 5 tables
- **Staging**: Same as raw (views)
- **Marts**: 5,062 dimension rows + 35,980 fact rows
- **Database Size**: ~5-10 MB (DuckDB)

## Evidence Dashboards

The project includes interactive dashboards built with Evidence:

### Available Dashboards

1. **Overview** (`/`) - Key metrics, revenue trends, category performance
2. **Sales** (`/sales`) - Daily/monthly sales, country analysis, recent orders
3. **Products** (`/products`) - Product performance, category trends, price analysis
4. **Customers** (`/customers`) - Customer segments, lifetime value, acquisition trends

### Running Evidence

```bash
cd evidence
npm install       # First time only
npm run sources   # Build data sources
npm run dev       # Start dev server
```

Then open http://localhost:3000 to view dashboards.

**Note**: Evidence connects to the DuckDB warehouse at `../warehouse/data.duckdb` and queries the `marts` schema through pass-through SQL files (`fct_orders.sql`, `dim_customers.sql`, `dim_products.sql`).

### Building Evidence (Static Site)

```bash
# Using Airflow DAG
cd airflow
uv run python dags/build_evidence.py

# Or build directly
cd evidence
npm run build
```

## Local Setup Notes (WSL + Airflow + DuckDB)

### What this project does (1‑minute overview)

This repo is a **local, reproducible data platform demo**:

1. **Generate synthetic source data** (CSV files)
2. **Ingest data with Airflow DAGs** into DuckDB
3. **Transform data with dbt** (staging → marts)
4. **Query analytics tables** (`marts.fct_orders`)
5. **Visualize results with Evidence**

There are **no external services** (no Postgres, no Snowflake, no cloud). Everything runs locally.

---

### Important concepts (before debugging)

* **Airflow = workflow DSL, not a daemon**

  * DAGs describe *what should happen*
  * They do **not** automatically run unless scheduled or triggered
  * `./setup.sh` uses `airflow dags test` → runs DAGs **once** for validation

* **DuckDB = embedded warehouse**

  * Data is stored in `warehouse/data.duckdb`
  * No server, no credentials

* **dbt = transformations only**

  * Reads from DuckDB
  * Builds `staging` views and `marts` tables

---

### Common setup gotchas (based on real issues)

#### 1. WSL vs Windows confusion

* Commands must run **inside WSL (Ubuntu)**
* Windows paths appear as:

  ```bash
  /mnt/c/Users/<you>/Desktop/Github/mini-data-platform
  ```

#### 2. Python version mismatch

* Use **Python 3.11** (required by Airflow/dbt)
* Verify:

  ```bash
  python3 --version
  ```

#### 3. `duckdb` command not found

* DuckDB CLI is optional but helpful
* Install:

  ```bash
  sudo snap install duckdb
  ```

#### 4. SQL commands must run *inside DuckDB*

Wrong:

```bash
SELECT * FROM marts.fct_orders;
```

Correct:

```bash
duckdb warehouse/data.duckdb
D SHOW TABLES;
D SELECT COUNT(*) FROM marts.fct_orders;
```

#### 5. Evidence shows “no sources found”

* You must generate sources first:

  ```bash
  cd evidence
  npm run sources
  npm run dev
  ```

---

### How Airflow is used here

* DAGs live in `airflow/dags/`
* Each DAG represents **one pipeline** (products, users, transactions, dbt, etc.)
* During setup:

  * DAGs are executed with `airflow dags test`
  * This validates logic without running a scheduler

If you want **real scheduling** later:

```bash
airflow scheduler
airflow webserver
```

---

### What is already complete

✅ Synthetic data generation
✅ Airflow ingestion DAGs
✅ DuckDB warehouse
✅ dbt staging + marts models
✅ Data validation tests
✅ CLI agent schema discovery

---

### Optional next steps (learning‑focused)

* Explore data:

  ```sql
  SELECT * FROM marts.fct_orders LIMIT 10;
  ```

* Modify a dbt model (`dbt_project/models/marts/`)

* Add a new DAG (e.g. incremental load)

* Add a metric/dashboard in Evidence

---

### Mental model summary

```
CSV sources
   ↓
Airflow DAGs (orchestration)
   ↓
DuckDB (warehouse)
   ↓
dbt (transformations)
   ↓
Evidence (analytics)
```

This setup is **correct, complete, and working as intended**.

## 🧭 How This Project Works (Short Version)

### TL;DR

```
Raw CSV → Airflow DAG → DuckDB → dbt → Evidence
```

### Key Things to Know

* **Airflow = workflow DSL, not a daemon**

  * DAGs define *what to run*
  * This repo runs DAGs via `airflow dags test` (one-off, no scheduler)

* **DuckDB is embedded**

  * No server
  * SQL must run *inside* `duckdb data.duckdb`, not bash

* **Evidence needs data first**

  * Run `npm run sources` before `npm run dev`

### Common Gotchas (real issues hit)

* SQL like `SHOW SCHEMAS;` must be run inside DuckDB
* Evidence error `no sources found` = data/dbt not generated yet
* WSL users must run everything from Linux shell (`/mnt/c/...` paths)
* Python version mismatch can break setup (use Python 3.11)

### Status

✅ Ingestion, transforms, marts, and dashboards are complete.
