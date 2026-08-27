# dbt path (optional)

The SQL notebooks in `../notebooks/` build the whole lakehouse with zero local tooling and are the
recommended route on Free Edition. This folder is for teams that would rather keep the transforms in
a dbt project.

## Files

| File | Use |
|---|---|
| `profiles.yml` | dbt profile targeting a Databricks SQL warehouse (`type: databricks`). Copy to `~/.dbt/profiles.yml` or pass `--profiles-dir`. |
| `sources_databricks.yml` | dbt `sources:` for the bronze layer — put at `elecmart/models/BRONZE/sources.yml`. |

## What dbt does / doesn't do

- dbt does **not** ingest files. Run `../notebooks/00_setup.sql` and `../notebooks/01_bronze.sql`
  first to create the catalog/schemas/volume and load `elecmart.bronze.*`.
- `dbt build` then materialises the SILVER and GOLD models and runs the tests.
- Run `../notebooks/04_gold_bi_marts.sql` afterwards for the `bi_*` views the dashboard reads.

## Setup

```bash
pip install dbt-core dbt-databricks

export DATABRICKS_HOST=dbc-xxxxxxxx-xxxx.cloud.databricks.com   # no https://
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxxxxxxxxxx    # SQL warehouse ▸ Connection details
export DATABRICKS_TOKEN=dapi...                                 # User settings ▸ Developer ▸ Access tokens

cd elecmart
dbt deps        # if packages.yml present
dbt build       # SILVER + GOLD models + all tests
```

## Databricks SQL notes for the models

Most models need no change. A few functions to be aware of:

| Need | Databricks SQL |
|---|---|
| Age in whole years | `date_diff(YEAR, birth_date, current_date())` — counts elapsed years anchored on the birthday (true age). Use `year(current_date()) - year(birth_date)` if you want a plain calendar-year difference instead. |
| `yyyyMM` integer key from a date | `cast(date_format(d, 'yyyyMM') as int)` |
| Last day / day-of-month | `day(last_day(`date`))` — back-tick `date` so it is not read as the type keyword |
| Cast | `cast(x as int)` / `x::int` both work; `TIMESTAMP_NTZ` is a supported type |

`initcap`, `lower`, `trim`, `row_number() OVER (...)`, `||` string concat (implicitly casts numerics),
`coalesce`, `nullif`, `round`, `case` all behave as expected.

## dbt_project.yml

No change needed. `+materialized: table` creates Delta tables. `+store_failures: true` on
`data_tests` is supported by dbt-databricks — failing rows land in schema `_test_failures`.
Optionally add `+file_format: delta` and, for large facts, `+incremental_strategy: merge` later.
