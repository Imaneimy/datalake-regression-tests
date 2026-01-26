"""
Regression Tests — DataFrame Comparator (release-over-release comparison)
XRAY IDs: TC-COMP-001 → TC-COMP-006
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from comparators.dataframe_comparator import (
    compare_schemas,
    compare_row_counts,
    compare_content,
    compare_numeric_stats,
)


def make_ref(spark):
    data = [("C001", "electronics", 500.0), ("C002", "clothing", 200.0)]
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("category", StringType()),
        StructField("total_amount", DoubleType()),
    ])
    return spark.createDataFrame(data, schema)


# ---------------------------------------------------------------------------
# TC-COMP-001 | Identical schemas pass
# ---------------------------------------------------------------------------
def test_compare_schemas_identical(spark):
    ref = make_ref(spark)
    result = compare_schemas(ref, ref)
    assert result.passed


# ---------------------------------------------------------------------------
# TC-COMP-002 | Removed column is detected
# ---------------------------------------------------------------------------
def test_compare_schemas_missing_column(spark):
    ref = make_ref(spark)
    cur = ref.drop("category")
    result = compare_schemas(cur, ref)
    assert not result.passed
    assert any("category" in e for e in result.errors)


# ---------------------------------------------------------------------------
# TC-COMP-003 | Row count within tolerance passes
# ---------------------------------------------------------------------------
def test_compare_row_counts_within_tolerance(spark):
    ref = make_ref(spark)
    data_cur = [("C001", "electronics", 500.0), ("C002", "clothing", 200.0), ("C003", "food", 100.0)]
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("category", StringType()),
        StructField("total_amount", DoubleType()),
    ])
    cur = spark.createDataFrame(data_cur, schema)
    result = compare_row_counts(cur, ref, tolerance_pct=60.0)
    assert result.passed


# ---------------------------------------------------------------------------
# TC-COMP-004 | Row count exceeding tolerance fails
# ---------------------------------------------------------------------------
def test_compare_row_counts_exceeds_tolerance(spark):
    ref = make_ref(spark)
    data_cur = [
        ("C001", "electronics", 500.0),
        ("C002", "clothing", 200.0),
        ("C003", "food", 100.0),
        ("C004", "food", 80.0),
        ("C005", "clothing", 60.0),
    ]
    schema = StructType([
        StructField("customer_id", StringType()),
        StructField("category", StringType()),
        StructField("total_amount", DoubleType()),
    ])
    cur = spark.createDataFrame(data_cur, schema)
    result = compare_row_counts(cur, ref, tolerance_pct=5.0)
    assert not result.passed


# ---------------------------------------------------------------------------
# TC-COMP-005 | Content diff detects removed rows
# ---------------------------------------------------------------------------
def test_compare_content_removed_rows(spark):
    ref = make_ref(spark)
    cur = spark.createDataFrame(
        [("C001", "electronics", 500.0)],
        ref.schema
    )
    result = compare_content(cur, ref, key_cols=["customer_id"])
    assert not result.passed
    assert result.metrics["rows_removed"] == 1


# ---------------------------------------------------------------------------
# TC-COMP-006 | Statistical drift beyond tolerance fails
# ---------------------------------------------------------------------------
def test_compare_numeric_stats_drift(spark):
    ref = make_ref(spark)
    drifted_data = [("C001", "electronics", 600.0), ("C002", "clothing", 300.0)]
    cur = spark.createDataFrame(drifted_data, ref.schema)
    result = compare_numeric_stats(cur, ref, numeric_cols=["total_amount"], tolerance_pct=5.0)
    assert not result.passed
