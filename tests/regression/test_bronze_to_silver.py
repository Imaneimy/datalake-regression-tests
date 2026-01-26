"""
Regression Tests — Bronze → Silver Layer
XRAY IDs: TC-REG-001 → TC-REG-010
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

import pytest
from pyspark.sql import functions as F
from datalake.layers import SILVER_SCHEMA, bronze_to_silver
from comparators.dataframe_comparator import (
    compare_schemas,
    compare_row_counts,
    validate_not_null_regression,
)


# ---------------------------------------------------------------------------
# TC-REG-001 | Silver schema matches expected definition
# ---------------------------------------------------------------------------
def test_silver_schema_matches_expected(spark, silver_data):
    """Silver DataFrame must conform to SILVER_SCHEMA after transformation."""
    from validators.schema_regression import assert_schema_subset
    actual_cols = {f.name for f in silver_data.schema.fields}
    expected_cols = {"event_id", "customer_id", "product_id", "category",
                     "amount", "event_date", "status", "_ingestion_ts"}
    assert expected_cols.issubset(actual_cols), f"Missing: {expected_cols - actual_cols}"


# ---------------------------------------------------------------------------
# TC-REG-002 | NULL customer_id rows are filtered out
# ---------------------------------------------------------------------------
def test_silver_no_null_customer_id(spark, silver_data):
    """Rows with NULL customer_id must not pass to Silver layer."""
    nulls = silver_data.filter(F.col("customer_id").isNull()).count()
    assert nulls == 0, f"Found {nulls} NULL customer_id in Silver"


# ---------------------------------------------------------------------------
# TC-REG-003 | Negative amounts are filtered out
# ---------------------------------------------------------------------------
def test_silver_no_negative_amounts(spark, silver_data):
    """Amounts ≤ 0 must be excluded from the Silver layer."""
    negatives = silver_data.filter(F.col("amount") <= 0).count()
    assert negatives == 0, f"Found {negatives} non-positive amounts in Silver"


# ---------------------------------------------------------------------------
# TC-REG-004 | Only valid statuses reach Silver
# ---------------------------------------------------------------------------
def test_silver_valid_statuses_only(spark, silver_data):
    """Only COMPLETED, PENDING, REFUNDED statuses are allowed in Silver."""
    valid = {"COMPLETED", "PENDING", "REFUNDED"}
    invalid = (
        silver_data
        .filter(~F.col("status").isin(list(valid)))
        .count()
    )
    assert invalid == 0, f"Found {invalid} rows with invalid status in Silver"


# ---------------------------------------------------------------------------
# TC-REG-005 | Duplicates are removed (event_id is unique)
# ---------------------------------------------------------------------------
def test_silver_no_duplicate_event_ids(spark, silver_data):
    """event_id must be unique in Silver (deduplication applied)."""
    total = silver_data.count()
    distinct = silver_data.dropDuplicates(["event_id"]).count()
    assert total == distinct, f"Duplicates found: {total - distinct} extra row(s)"


# ---------------------------------------------------------------------------
# TC-REG-006 | Amount type is Double in Silver
# ---------------------------------------------------------------------------
def test_silver_amount_is_double(spark, silver_data):
    """Amount must be cast to DoubleType in Silver."""
    from pyspark.sql.types import DoubleType
    dtype = silver_data.schema["amount"].dataType
    assert isinstance(dtype, DoubleType), f"Expected DoubleType, got {dtype}"


# ---------------------------------------------------------------------------
# TC-REG-007 | event_date is parsed to Timestamp
# ---------------------------------------------------------------------------
def test_silver_event_date_is_timestamp(spark, silver_data):
    """event_date must be TimestampType in Silver (parsed from string)."""
    from pyspark.sql.types import TimestampType
    dtype = silver_data.schema["event_date"].dataType
    assert isinstance(dtype, TimestampType), f"Expected TimestampType, got {dtype}"


# ---------------------------------------------------------------------------
# TC-REG-008 | Category is uppercased and trimmed
# ---------------------------------------------------------------------------
def test_silver_category_uppercased(spark, silver_data):
    """Category values must be uppercased and trimmed in Silver."""
    non_upper = silver_data.filter(F.col("category") != F.upper(F.trim(F.col("category")))).count()
    assert non_upper == 0, f"{non_upper} category values not properly normalized"


# ---------------------------------------------------------------------------
# TC-REG-009 | Silver row count matches expected after anomaly removal
# ---------------------------------------------------------------------------
def test_silver_row_count(spark, bronze_data, silver_data):
    """
    Bronze has 12 rows with:
    - 1 NULL customer_id → removed
    - 1 negative amount → removed
    - 1 invalid status (CANCELLED) → removed
    - 1 duplicate event_id → removed
    Expected Silver count: 8
    """
    expected = 8
    actual = silver_data.count()
    assert actual == expected, f"Expected {expected} rows in Silver, got {actual}"


# ---------------------------------------------------------------------------
# TC-REG-010 | Audit column _ingestion_ts is populated
# ---------------------------------------------------------------------------
def test_silver_ingestion_ts_populated(spark, silver_data):
    """_ingestion_ts must be non-null for every Silver row."""
    nulls = silver_data.filter(F.col("_ingestion_ts").isNull()).count()
    assert nulls == 0, f"{nulls} rows missing _ingestion_ts"
