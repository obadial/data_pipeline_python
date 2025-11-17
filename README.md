# gcp-sales-pipeline

Python pipeline to:

- Read **products** from BigQuery (`bm_mock_data.products`)
- Read **sales** from GCS (`gs://bm_mock_sales/sales_YYYY-MM-DD.parquet`)
- Filter sales using a reference date with different granularities:
  - `day`, `month`, `quarter`, `year`
- Join products and sales on `product_id`
- Optionally filter by `brand` and `product_id`
- Export the final dataset to a date-suffixed file (Parquet or CSV)

---

## 1. Project layout

```text
gcp-sales-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml           # GitHub Actions CI (lint, unit, integration)
├── .gitignore               # Excludes .env, data, venv, etc.
├── .pre-commit-config.yaml  # Local checks (black, flake8, tests)
├── pyproject.toml
├── README.md
├── src/
│   └── gcp_sales_pipeline/
│       ├── __init__.py
│       ├── bq_client.py
│       ├── cli.py
│       ├── exceptions.py
│       ├── filters.py
│       ├── gcs_client.py
│       └── pipeline.py
└── tests/
    ├── unit/
    │   └── test_filters.py
    └── integration/
        └── test_pipeline_integration.py
````

At runtime (not committed to git):

```text
.env                       # contains GOOGLE_APPLICATION_CREDENTIALS
bot-sandbox-interviews-sa.json               # service account key (do NOT commit)
data/
  └── export/              # output files (ignored)
```

---

## 2. GCS layout

Sales files are expected to live in:

```text
gs://bm_mock_sales/sales_YYYY-MM-DD.parquet
```

One file per day, e.g.:

* `gs://bm_mock_sales/sales_2025-01-01.parquet`
* `gs://bm_mock_sales/sales_2025-01-02.parquet`
* etc.

Each file should contain at least:

* `product_id` (STRING)
* `price` (FLOAT)
* `quantity` (INTEGER)
* `sold_at` (TIMESTAMP)
* `order_id` (STRING)

The pipeline:

1. Computes the date range corresponding to the requested granularity
   (day / month / quarter / year).
2. Loads daily Parquet files in that range.
3. Filters again on `sold_at` to match exactly the period.
4. Joins with product data from BigQuery.

---

## 3. BigQuery layout

Products are read from table:

```text
test.products
```

Expected schema:

* `product_id` (STRING)
* `product_name` (STRING)
* `category` (STRING)
* `brand` (STRING)
* `condition` (STRING)

---

## 4. Requirements

* Python 3.10+
* Access to a GCP project with:

  * BigQuery dataset `bm_mock_data` and table `products`
  * GCS bucket `bm_mock_sales` with daily Parquet sales files
* A **service account JSON** file, e.g. `bot-sandbox-interviews-sa.json`

---

## 5. Environment configuration

Create a `.env` file at the **project root** (this file is ignored by git):

```env
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/bot-sandbox-interviews-sa.json
```

> ⚠️ Do **not** commit `.env` or `bot-sandbox-interviews-sa.json`.
> `.gitignore` already excludes `.env` and typical secrets/artefacts.

The CLI automatically loads `.env` via `python-dotenv`.

---

## 6. Installation

### 6.1. With `uv`

```bash
uv venv
source .venv/bin/activate

uv pip install -e ".[dev]"
```

### 6.2. With plain `pip`

```bash
python -m venv .venv
source .venv/bin/activate

pip install -e ".[dev]"
```

This installs:

* Runtime deps: `pandas`, `google-cloud-bigquery`, `google-cloud-storage`,
  `pyarrow`, `python-dotenv`
* Dev tools: `pytest`, `pytest-mock`, `black`, `flake8`, `pre-commit`

---

## 7. Running the pipeline

The CLI entrypoint is `gcp-sales-pipeline`, defined in `pyproject.toml`.

### 7.1. Arguments

* `--date` (required): reference date, `YYYY-MM-DD`
* `--project-id`: GCP project ID
* `--granularity`: `day` (default), `month`, `quarter`, `year`
* `--brand`: may be passed multiple times to filter by several brands
* `--product-id`: may be passed multiple times to filter by several product IDs
* `--bq-dataset`: BigQuery dataset (default: `bm_mock_data`)
* `--bq-table`: products table name (default: `products`)
* `--gcs-bucket`: GCS bucket (default: `bm_mock_sales`)
* `--output-dir`: export directory (default: `data/export`)
* `--output-format`: `parquet` (default) or `csv`
* `--max-sales-files`: max number of daily files (days) to load (default: `500`)
* `--log-level`: `DEBUG`, `INFO`, `WARNING`, `ERROR` (default: `INFO`)

### 7.2. Examples

#### Day level

```bash
gcp-sales-pipeline \
  --date 2025-11-17
```

#### Month level

```bash
gcp-sales-pipeline \
  --date 2025-03-01 \
  --granularity month
```

#### Quarter level

```bash
gcp-sales-pipeline \
  --date 2025-04-15 \
  --granularity quarter
```

#### Year level with filters

```bash
gcp-sales-pipeline \
  --project-id my-gcp-project \
  --date 2025-01-01 \
  --granularity year \
  --brand Nike \
  --brand Adidas \
  --product-id P12345 \
  --product-id P67890
```

Semantics of filters:

* `product_id` is in `{P12345, P67890}`
* `sold_at` is in the date range corresponding to the requested granularity.

---

## 8. Output files

By default, exports are written to:

```text
data/export/
```

With filenames like:

* `sales_products_day_2025-11-17.parquet`
* `sales_products_month_2025-03.parquet`
* `sales_products_quarter_2025-Q2.parquet`
* `sales_products_year_2025.parquet`

You can switch to CSV using `--output-format csv`.

---

## 9. Error handling

The code uses custom exception types:

* `DataLoadError`
  For issues while reading from BigQuery / GCS (network, permissions, etc.).

* `DataQualityError`
  For schema issues (e.g. missing required columns in the BigQuery table).

* `TooManyFilesError`
  When the requested date range exceeds `--max-sales-files` daily files.

The CLI maps these to exit codes:

* `1` → data load / data quality problem
* `2` → too many files / too large date range
* `99` → unexpected error

Error details are also logged using Python’s `logging` module.

---

## 10. Tests

### 10.1. Unit tests

```bash
pytest tests/unit
```

Currently focused on:

* Date / granularity filtering logic.

### 10.2. Integration tests (mocked)

```bash
pytest tests/integration
```

These tests:

* Mock BigQuery and GCS clients.
* Run the full pipeline.
* Check that:

  * join between products and sales works
  * the exported file is produced
  * the schema matches expectations.

---

## 11. Pre-commit hooks

The repo includes a `.pre-commit-config.yaml` running:

* `black` (formatter)
* `flake8` (linter)
* unit tests (`pytest tests/unit`)
* integration tests (`pytest tests/integration`)

Install hooks:

```bash
pre-commit install
```

After that, every commit will automatically:

1. Format code with `black`
2. Lint with `flake8`
3. Run unit tests
4. Run integration tests

If any step fails, the commit is blocked.

---

## 12. CI with GitHub Actions

The workflow file:

```text
.github/workflows/ci.yml
```

Defines three jobs:

1. **lint**

   * runs `black --check` and `flake8`

2. **unit-tests**

   * runs `pytest tests/unit`

3. **integration-tests**

   * runs `pytest tests/integration`

Jobs run on pushes and pull requests, ensuring consistent quality between
local development and CI.

