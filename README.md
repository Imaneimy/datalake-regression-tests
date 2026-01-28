# datalake-regression-tests

One thing I noticed working on ETL pipelines is that teams often break things between releases without realizing it — a column gets renamed, a filter gets tightened, an aggregation changes. This project is my attempt at building a regression harness that catches that kind of drift automatically.

The architecture follows the Medallion pattern (Bronze -> Silver -> Gold) which is standard in Databricks/Datalake environments. Each layer has its own set of checks, and there's also a DataFrame comparator that you can point at two versions of the same output to detect schema changes, row count anomalies, or statistical drift.

## Structure

```
src/
  datalake/
    layers.py                  # the three transformations: bronze_to_silver, silver_to_gold
  comparators/
    dataframe_comparator.py    # compare two DataFrames across schema, count, content, stats

tests/
  smoke/
    test_smoke.py              # 4 fast checks — run these first before the full suite
  regression/
    test_bronze_to_silver.py   # 10 tests: nulls, types, dedup, status filter, row count
    test_silver_to_gold.py     # 8 tests: reconciliation, avg consistency, ordering
    test_comparator.py         # 6 tests: schema drift, count delta, content diff, stat drift
```

## How to run

```bash
pip install -r requirements.txt

# Start with smoke tests — if these fail, don't bother with the rest
pytest tests/smoke/ -v

# Full suite
pytest tests/regression/ -v
```

## What the layers do

Bronze is raw data, untouched. Silver is where the cleaning happens: nulls on key columns get dropped, amounts are cast to double, dates parsed to timestamp, invalid statuses filtered out, duplicates removed. Gold aggregates the cleaned data — only COMPLETED transactions, grouped by customer and category.

The test data is set up with four deliberate anomalies in Bronze (a null customer_id, a negative amount, a CANCELLED status that should be filtered, and a duplicate event_id) to make sure the Silver transformation actually handles all of them.

## The comparator

`dataframe_comparator.py` has four functions you can use independently:

- `compare_schemas` — detects added, removed, or retyped columns
- `compare_row_counts` — flags delta above a configurable % threshold
- `compare_content` — left-anti join on key columns to find rows that appeared or disappeared
- `compare_numeric_stats` — checks SUM and MEAN drift on numeric columns

## Stack

PySpark 3.4, Pytest
