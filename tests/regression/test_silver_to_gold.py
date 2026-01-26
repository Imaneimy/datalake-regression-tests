"""
Regression Tests — Silver → Gold Layer
XRAY IDs: TC-GOLD-001 → TC-GOLD-008
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../src"))

import pytest
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# TC-GOLD-001 | Gold only contains COMPLETED transactions
# ---------------------------------------------------------------------------
def test_gold_only_completed(spark, silver_data, gold_data):
    """Gold aggregation must be based solely on COMPLETED transactions."""
    completed_customers = (
        silver_data
        .filter(F.col("status") == "COMPLETED")
        .select("customer_id").distinct()
    )
    gold_customers = gold_data.select("customer_id").distinct()
    extra = gold_customers.join(completed_customers, on="customer_id", how="left_anti").count()
    assert extra == 0, f"Gold contains {extra} customer(s) with no COMPLETED transactions"


# ---------------------------------------------------------------------------
# TC-GOLD-002 | total_amount is always positive
# ---------------------------------------------------------------------------
def test_gold_total_amount_positive(spark, gold_data):
    non_positive = gold_data.filter(F.col("total_amount") <= 0).count()
    assert non_positive == 0, f"{non_positive} non-positive total_amount in Gold"


# ---------------------------------------------------------------------------
# TC-GOLD-003 | transaction_count is a positive integer string
# ---------------------------------------------------------------------------
def test_gold_transaction_count_valid(spark, gold_data):
    invalid = gold_data.filter(F.col("transaction_count").cast("int") <= 0).count()
    assert invalid == 0, f"{invalid} rows with invalid transaction_count"


# ---------------------------------------------------------------------------
# TC-GOLD-004 | first_event ≤ last_event
# ---------------------------------------------------------------------------
def test_gold_event_dates_ordered(spark, gold_data):
    out_of_order = gold_data.filter(F.col("first_event") > F.col("last_event")).count()
    assert out_of_order == 0, f"{out_of_order} rows where first_event > last_event"


# ---------------------------------------------------------------------------
# TC-GOLD-005 | Each (customer_id, category) pair is unique in Gold
# ---------------------------------------------------------------------------
def test_gold_unique_customer_category(spark, gold_data):
    total = gold_data.count()
    distinct = gold_data.dropDuplicates(["customer_id", "category"]).count()
    assert total == distinct, f"{total - distinct} duplicate (customer, category) pairs in Gold"


# ---------------------------------------------------------------------------
# TC-GOLD-006 | Reconciliation: Gold totals ≈ Silver totals (COMPLETED)
# ---------------------------------------------------------------------------
def test_gold_reconciliation_with_silver(spark, silver_data, gold_data):
    """Sum of Gold total_amount must equal sum of Silver amount (COMPLETED only)."""
    silver_sum = (
        silver_data
        .filter(F.col("status") == "COMPLETED")
        .select(F.sum("amount")).first()[0]
    ) or 0.0

    gold_sum = gold_data.select(F.sum("total_amount")).first()[0] or 0.0

    drift = abs(silver_sum - gold_sum)
    assert drift < 0.01, f"Reconciliation failed: Silver={silver_sum:.2f} vs Gold={gold_sum:.2f}"


# ---------------------------------------------------------------------------
# TC-GOLD-007 | avg_amount = total_amount / transaction_count
# ---------------------------------------------------------------------------
def test_gold_avg_amount_consistent(spark, gold_data):
    """Verify avg_amount is consistent with total_amount / transaction_count."""
    from pyspark.sql.functions import abs as spark_abs
    inconsistent = (
        gold_data
        .withColumn(
            "computed_avg",
            F.col("total_amount") / F.col("transaction_count").cast("double")
        )
        .filter(spark_abs(F.col("avg_amount") - F.col("computed_avg")) > 0.001)
        .count()
    )
    assert inconsistent == 0, f"{inconsistent} rows with inconsistent avg_amount"


# ---------------------------------------------------------------------------
# TC-GOLD-008 | Gold row count is positive
# ---------------------------------------------------------------------------
def test_gold_not_empty(spark, gold_data):
    assert gold_data.count() > 0, "Gold DataFrame is empty — aggregation failed"
