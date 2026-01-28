# Datalake Regression Testing — Medallion Architecture

Automated regression test suite for a Bronze / Silver / Gold Medallion Datalake architecture built with PySpark. Verifies that each pipeline release does not degrade data quality across layers. Includes a DataFrame comparator for detecting schema drift, row count anomalies, and statistical deviations between releases.

---

## Project structure

```
02_datalake_regression_tests/
├── src/
│   ├── datalake/
│   │   └── layers.py                  # Bronze -> Silver -> Gold transformations
│   └── comparators/
│       └── dataframe_comparator.py    # Release-over-release DataFrame comparison
├── tests/
│   ├── conftest.py                    # Spark fixtures + sample datasets
│   ├── smoke/
│   │   └── test_smoke.py              # TC-SMOKE-001 to 004 — fast sanity checks
│   └── regression/
│       ├── test_bronze_to_silver.py   # TC-REG-001 to 010
│       ├── test_silver_to_gold.py     # TC-GOLD-001 to 008
│       └── test_comparator.py         # TC-COMP-001 to 006
├── data/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── reference/    # Reference snapshots for release N-1 comparison
└── docs/
```

---

## Setup

```bash
pip install -r requirements.txt

# Smoke tests — run first
pytest tests/smoke/ -v

# Full regression suite
pytest tests/regression/ -v

# All tests with HTML report
pytest --html=reports/regression_report.html
```

---

## Architecture

```
CSV/JSON -> [BRONZE] -> bronze_to_silver() -> [SILVER] -> silver_to_gold() -> [GOLD]
```

| Layer | Description | Rules applied |
|-------|-------------|---------------|
| Bronze | Raw data, as-is | No transformation |
| Silver | Cleaned and typed data | NULL filter, cast, dedup, valid statuses |
| Gold | Business aggregates | COMPLETED transactions only |

---

## Test strategy

| Type | Scope |
|------|-------|
| Smoke tests | Spark session, layer imports, no-exception checks |
| Regression — Bronze to Silver | NULL filtering, type casting, dedup, status validation, row count |
| Regression — Silver to Gold | Reconciliation, avg consistency, date ordering, uniqueness |
| Comparator | Schema drift, row count delta, content diff, statistical drift |

---

## Intentional anomalies in test data

| Anomaly | Layer | Test that detects it |
|---------|-------|----------------------|
| NULL customer_id | Bronze | TC-REG-002 |
| Negative amount -50.00 | Bronze | TC-REG-003 |
| Invalid status CANCELLED | Bronze | TC-REG-004 |
| Duplicate event_id E001 | Bronze | TC-REG-005 |

---

## Stack

PySpark 3.4+ / Pytest / Medallion Architecture

---

## Author

Imane Moussafir — Data & BI Engineer
