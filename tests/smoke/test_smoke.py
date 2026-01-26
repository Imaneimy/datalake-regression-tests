"""
Smoke Tests — Fast sanity checks on Datalake layer availability
XRAY IDs: TC-SMOKE-001 → TC-SMOKE-004
Run these first: if they fail, no point running the full regression suite.
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

import pytest
from datalake.layers import bronze_to_silver, silver_to_gold, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA


# ---------------------------------------------------------------------------
# TC-SMOKE-001 | Spark session is reachable
# ---------------------------------------------------------------------------
def test_spark_session_alive(spark):
    assert spark is not None
    assert spark.sparkContext.defaultParallelism > 0


# ---------------------------------------------------------------------------
# TC-SMOKE-002 | Bronze → Silver transform completes without exception
# ---------------------------------------------------------------------------
def test_bronze_to_silver_no_exception(spark, bronze_data):
    try:
        silver = bronze_to_silver(bronze_data)
        silver.count()
    except Exception as exc:
        pytest.fail(f"bronze_to_silver raised: {exc}")


# ---------------------------------------------------------------------------
# TC-SMOKE-003 | Silver → Gold transform completes without exception
# ---------------------------------------------------------------------------
def test_silver_to_gold_no_exception(spark, silver_data):
    try:
        gold = silver_to_gold(silver_data)
        gold.count()
    except Exception as exc:
        pytest.fail(f"silver_to_gold raised: {exc}")


# ---------------------------------------------------------------------------
# TC-SMOKE-004 | Layer schemas are importable and non-empty
# ---------------------------------------------------------------------------
def test_schemas_defined():
    assert len(BRONZE_SCHEMA.fields) > 0
    assert len(SILVER_SCHEMA.fields) > 0
    assert len(GOLD_SCHEMA.fields) > 0
