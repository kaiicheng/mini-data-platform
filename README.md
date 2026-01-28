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

# CLI Analytics Agent

This repository extends the reference mini data platform with a **production‑minded CLI** for answering ad‑hoc analytics questions on **curated marts data**.

The emphasis is on **deterministic queries**, **schema awareness**, and **explicit handling of unsupported questions** — not free‑form SQL or LLM‑generated logic.

The goal of this agent **is not to maximize coverage**, but to **provide a predictable**, **auditable analytics interface that behaves safely under ambiguity**.

---

### Example usage

```bash
# Validate warehouse connectivity and marts availability
python -m cli.main test
```

```bash
# Compute total sales revenue
python -m cli.main sales-cmd --start 2024-01-01 --end 2024-12-31
```

```bash
# List top-N products by units sold
python -m cli.main top-products-cmd --n 5
```

```bash
# Discover marts schema metadata
python -m cli.main schema
```

```bash
# Ask a supported natural-language question (rule-based routing)
python -m cli.main ask "How much in sales did we do last quarter?"
```

```bash
# Ask about customer metrics inferred from marts metadata
python -m cli.main ask "How many customers do we have?"
```

```bash
# Inspect warehouse data availability (min/max transaction_date)
python -m cli.main ask "What is the available data range?"
```

```bash
# List users with the most orders (deterministic, schema-driven)
python -m cli.main ask "Which customers have the most orders?"
```

#### Data availability & guardrails

The CLI agent validates **data availability before executing any query**. All analytics are bounded by the minimum and maximum `transaction_date` present in `marts.fct_orders`.

If a requested period falls outside this range, the agent **explicitly rejects the request** instead of returning misleading results (e.g. zero revenue).

Example:

```bash
python -m cli.main ask "How much in sales did we do last quarter?"
```

```
❌ Requested period 2025-10-28 → 2026-01-26 is outside available data range (2020-01-20 → 2024-12-31)
```

This mirrors production analytics systems, where silent failures are more dangerous than hard errors.

---

### What this CLI does

The CLI provides a safe analytics interface on top of the existing warehouse:

* Answers common business questions using **explicit query handlers**
* Restricts access to **marts tables only**
* Infers behavior from **schema metadata**, not hard‑coded assumptions

---

### Supported analytics

Currently supported, deterministic primitives:

* **Sales revenue** over a specified date range
* **Top‑N products** by units sold
* **Schema discovery** for marts tables and columns

Each capability maps to a single, auditable query.

---

### Design principles

* **Deterministic** – one question → one known query
* **Schema‑aware** – inspects warehouse metadata
* **Marts‑only** – avoids raw or intermediate data
* **Defensive** – unsupported requests are rejected explicitly
* **Explicit rejection over guessing** – when business semantics or required data are missing, the agent explains *why* a question cannot be answered instead of producing heuristic or fabricated results

---

### Intentional limitations

Some prompt examples are intentionally not supported yet:

* **Product pair analysis** – requires order‑item level granularity
* **Customer lifetime value** – requires an agreed business definition
* **Anomaly detection** – requires baselines and alerting semantics

Rather than guessing, the CLI surfaces these limitations clearly.

---

### Pitfalls encountered (and lessons learned)

* **Python `date` vs `datetime` mismatches**
  DuckDB may return `datetime` objects even for date columns. All date bounds are normalized to `datetime.date` before validation to avoid subtle comparison bugs.

* **WSL vs Windows context**
  All commands must be run inside the Linux (WSL) environment. Paths such as `/mnt/c/...` indicate Windows-mounted directories, but tooling like Airflow, DuckDB, and dbt must execute from the Linux shell.

---

### Future extensions (design notes)

The current implementation intentionally limits scope to deterministic, auditable analytics.
If given more time, the system could be extended in the following directions:

1. **Richer intent routing**
   Expand rule-based NLP into a lightweight intent registry or pattern matcher, while keeping SQL generation deterministic.

2. **Explicit time range parsing**
   Parse expressions like "last quarter" or "in 2023" into validated date ranges, instead of relying on a fixed default window.

3. **Additional analytics primitives**
   Add new deterministic query handlers (e.g. customer metrics, CLV with a defined formula) without coupling NLP logic to SQL.

4. **Explainable anomaly detection**
   Introduce simple, interpretable baselines (e.g. rolling averages) before attempting any automated alerting.

5. **LLM-assisted intent interpretation**
   Use an LLM only to classify intent and extract parameters, never to generate SQL directly.

6. **Alternative interfaces**
   Expose the same analytics primitives via a web API or BI layer (e.g. Evidence) without duplicating business logic.

# Appendix: Local Data Platform Setup (Reference)

This appendix provides **context only** for the underlying local data platform used by the CLI agent. It is **not required** to understand the CLI design, but explains where the data comes from and how the marts are produced.

---

### Platform components

The reference mini data platform consists of four layers:

1. **Synthetic data generation**
   CSV files are generated locally to simulate source systems (users, products, transactions).

2. **Airflow (orchestration)**
   Airflow DAGs ingest CSV sources into DuckDB and run dbt transformations. In this repo, DAGs are executed via `airflow dags test` for reproducibility rather than a long-running scheduler.

3. **DuckDB (warehouse)**
   DuckDB acts as an embedded analytical warehouse. All data lives in a single file:

   ```text
   warehouse/data.duckdb
   ```

   There is no server process, credentials, or network dependency.

4. **dbt (transformations)**
   dbt models transform raw and staging tables into curated marts (`marts.*`), which are the **only tables queried by the CLI agent**.

---

### Why the CLI queries marts only

The CLI is intentionally restricted to marts tables because they:

* Represent validated, business-facing datasets
* Have stable schemas and agreed semantics
* Are safe for ad-hoc analytics without leaking raw data

This mirrors how production analytics systems typically expose data to downstream consumers.

---

### Relationship to Evidence

Evidence is included in the reference platform as an example presentation layer. While the CLI does not depend on Evidence, both layers query the **same marts tables**, ensuring consistent metrics across interfaces.

If extended, the CLI’s analytics primitives could be exposed directly through Evidence without duplicating business logic.

---

### Pipeline summary

```
CSV sources
   ↓
Airflow DAGs (ingestion + dbt)
   ↓
DuckDB warehouse
   ↓
Curated marts (marts.*)
   ↓
CLI analytics agent / Evidence
```

This setup is intentionally local, reproducible, and dependency-light, making it suitable for demonstrations, experimentation, and deterministic analytics tooling.

---

### Pitfalls encountered (and lessons learned)

The following issues were encountered while working with the reference platform and are documented here to save future time:

* **WSL vs Windows context**
  All commands must be run inside the Linux (WSL) environment. Paths such as `/mnt/c/...` indicate Windows-mounted directories, but tooling like Airflow, DuckDB, and dbt must execute from the Linux shell.

* **Python environment isolation (PEP 668)**
  System Python on recent Linux distributions may block `pip install` by default. Creating and activating a dedicated virtual environment (`python3.11 -m venv .venv`) is required to install dependencies cleanly.

* **DuckDB SQL context**
  SQL commands such as `SHOW SCHEMAS;` or `SELECT * FROM marts.fct_orders;` must be executed *inside* the DuckDB CLI or through a DuckDB connection, not directly in the shell.

* **Airflow execution model**
  Airflow DAGs describe workflows but do not run automatically. This repo uses `airflow dags test` to execute pipelines once for validation, which avoids running a scheduler while still exercising the full DAG logic.

* **Evidence data availability**
  Evidence dashboards will report `no sources found` until dbt models have been built and `npm run sources` has been executed. This is expected behavior, not a misconfiguration.

Documenting these constraints helped clarify the operational boundaries of the platform and informed the CLI’s defensive, schema-aware design.
