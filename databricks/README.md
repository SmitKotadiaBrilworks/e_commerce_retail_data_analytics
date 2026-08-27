# Elecmart Retail Analytics — Databricks lakehouse

A medallion lakehouse for the Elecmart retail dataset on **Databricks Free Edition**, with an
**AI/BI dashboard** on top.

```
14 Parquet files
  └─ UC Volume  elecmart.bronze.landing
       └─ 01_bronze      → elecmart.bronze.*   (read_files → Delta)
            └─ 02_silver      → elecmart.silver.*   (clean / type / de-duplicate)
                 └─ 03_gold_star   → elecmart.gold.gold_*   (conformed star schema)
                      └─ 04_gold_bi_marts → elecmart.gold.bi_*   (metric views)
                           └─ AI/BI dashboard  +  Genie
                 └─ 05_data_quality   (assertions — fails the job on any violation)
```

---

## 1. Will this fit on a free Databricks account?

**Yes — comfortably.** Use **Databricks Free Edition** (`databricks.com/learn/free-edition`) —
**not** Community Edition, which has no SQL warehouse and cannot build dashboards.

The 14 Parquet files are in the repo working directory (`*.parquet`, ~420 MB total):

| file | rows | size |
|---|--:|--:|
| `fact_clickstream.parquet` | 14,500,000 | 351 MB |
| `fact_sale.parquet` | 1,799,836 | 34 MB |
| `fact_transaction.parquet` | 900,000 | 26 MB |
| `inventory.parquet` | 846,000 | 5 MB |
| `dim_customer.parquet` | 150,000 | 5 MB |
| 9 other dims | < 10k each | < 1 MB |

| Free Edition | This project needs |
|---|---|
| Serverless compute, per-account usage quota | Full build ≈ a few minutes; dashboard refresh is seconds |
| 1 SQL warehouse, `2X-Small` | Enough for `bi_*` views over ~18 M rows |
| Unity Catalog, Volumes, Workflows, AI/BI, Genie | all used here |
| 5 concurrent job tasks, 1 pipeline per type | pipeline is 6 sequential tasks |

`03_gold_star` keeps `gold_fact_clickstream` as a **view** (not a copy) so the 14.5 M-row table is
stored only once. Everything else is a Delta table.

If your workspace blocks `CREATE CATALOG`: find-and-replace `elecmart.` → `workspace.` in the 6
notebooks and drop the `CREATE CATALOG` line in `00_setup.sql`.

---

## 2. Connecting from this machine

Nothing is configured locally — no `databricks` CLI, no `~/.databrickscfg`, no `DATABRICKS_*`
env vars — so the notebooks can't be pushed from here yet. Two options:

**A. Manual UI import** (section 3) — no local setup.

**B. CLI / Asset Bundle:**

```bash
# install the CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# authenticate with a Personal Access Token
#   workspace ▸ User settings ▸ Developer ▸ Access tokens ▸ Generate new token
databricks configure --token          # Host: https://<workspace>.cloud.databricks.com
databricks current-user me            # verify

# deploy notebooks + dashboard + serverless job
cd databricks/workflow
databricks bundle deploy -t dev
databricks bundle run elecmart_pipeline -t dev
```

---

## 3. Run it (manual UI path — Free Edition)

1. **Sign in** to Free Edition.
2. **Import notebooks**: Workspace ▸ *Import* ▸ upload the 6 files in `notebooks/` (import as SQL notebooks).
3. **`00_setup`** → *Run all* (serverless). Creates catalog `elecmart`, schemas, volume
   `elecmart.bronze.landing`.
4. **Upload data**: Catalog ▸ `elecmart` ▸ `bronze` ▸ `landing` ▸ *Upload to volume* ▸ drag in all
   **14 `.parquet` files** (flat, no sub-folders). `fact_clickstream.parquet` is 351 MB — under the
   2 GB per-file UI limit.
5. Run **`01_bronze` → `02_silver` → `03_gold_star` → `04_gold_bi_marts` → `05_data_quality`** in order.
6. **Dashboard**: Dashboards ▸ *Create* ▸ ⋯ ▸ *Import dashboard* ▸
   `dashboards/elecmart_retail_analytics.lvdash.json`. Pick the SQL warehouse, *Publish*.
7. **Genie** (optional): New ▸ *Genie space* ▸ add tables `elecmart.gold.gold_*` ▸ ask
   "net revenue by month", "top 10 products by profit", "conversion rate by device".

To schedule: the bundle in `workflow/databricks.yml` wraps steps 3 + 5 in a 6-task serverless job.

---

## 4. Files

| Path | Purpose |
|---|---|
| `notebooks/00_setup.sql` | catalog, schemas, landing volume |
| `notebooks/01_bronze.sql` | `read_files()` → 14 bronze Delta tables |
| `notebooks/02_silver.sql` | clean / type / de-duplicate + light enrichment |
| `notebooks/03_gold_star.sql` | 6 dimensions + 4 facts + informational PK/FK for Genie |
| `notebooks/04_gold_bi_marts.sql` | `bi_*` metric views the dashboard reads |
| `notebooks/05_data_quality.sql` | 20 assertions; `raise_error` fails the job on any violation |
| `dashboards/elecmart_retail_analytics.lvdash.json` | 1-page AI/BI dashboard |
| `dbt/` | optional dbt path — see `dbt/README.md` |
| `workflow/databricks.yml` | Asset Bundle: notebooks + dashboard + serverless job |

---

## 5. Databricks SQL notes

- **Bronze load** — `read_files('/Volumes/…/x.parquet', format => 'parquet')` into
  `CREATE OR REPLACE TABLE`. Point at a folder or glob instead if a source is split into part-files.
- **Constraints** — Unity Catalog PK/FK are informational (not enforced); PK columns must be
  `NOT NULL`. They drive join discovery in Genie and AI/BI.
- **Functions** — `date_diff(YEAR, birth_date, current_date())` for age;
  `cast(date_format(d,'yyyyMM') as int)` for a month key; `` day(last_day(`date`)) `` (back-tick the
  `date` column). `initcap`, `lower`, `trim`, `row_number() OVER`, `||`, `coalesce`, `nullif`,
  `round` behave as expected.
- **`gold_fact_clickstream`** is a view to avoid a second copy of 14.5 M rows on Free Edition.

---

## 6. What's on the dashboard

One page, reads only `elecmart.gold.bi_*`:

- **KPI counters** — Net Revenue, Gross Profit, Gross Margin %, Completed Orders, Units Sold, AOV
- **Net revenue by month** (line) + **Revenue by category** (bar)
- **Revenue by channel**, **Top 15 products**, **Revenue by loyalty tier** (bars)
- **Session conversion rate by month** (line) + **Sessions by device** (bar)
- **Store performance** (table: revenue, profit, margin %, orders, AOV)
- **Campaign attributed revenue** (bar) + **Inventory turnover by category** (bar)

Covers executive, sales, clickstream, customer, campaign and inventory views. If the JSON import
ever rejects a widget spec, the `bi_*` views still stand on their own — add visuals in the UI in a
few minutes, or point Genie at them.
