"""
Pytest fixtures — SparkSession + sample datasets for regression tests.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("Regression_Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def bronze_data(spark):
    """Reference bronze dataset — 10 clean + intentional anomalies."""
    data = [
        ("E001", "C001", "P001", "electronics", "250.00", "2024-01-01", "COMPLETED"),
        ("E002", "C001", "P002", "clothing",    "89.99",  "2024-01-02", "COMPLETED"),
        ("E003", "C002", "P001", "electronics", "310.50", "2024-01-03", "COMPLETED"),
        ("E004", "C002", "P003", "food",        "45.00",  "2024-01-04", "PENDING"),
        ("E005", "C003", "P004", "electronics", "199.99", "2024-01-05", "COMPLETED"),
        ("E006", "C003", "P002", "clothing",    "120.00", "2024-01-06", "REFUNDED"),
        ("E007", "C004", "P001", "electronics", "500.00", "2024-01-07", "COMPLETED"),
        ("E008", "C004", "P005", "food",        "30.50",  "2024-01-08", "COMPLETED"),
        ("E009", None,   "P001", "electronics", "100.00", "2024-01-09", "COMPLETED"),   # NULL customer
        ("E010", "C005", "P002", "clothing",    "-50.00", "2024-01-10", "COMPLETED"),   # negative amount
        ("E011", "C005", "P003", "food",        "60.00",  "2024-01-11", "CANCELLED"),   # invalid status
        ("E001", "C001", "P001", "electronics", "250.00", "2024-01-01", "COMPLETED"),   # duplicate
    ]
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("product_id", StringType()),
        StructField("category", StringType()),
        StructField("amount", StringType()),
        StructField("event_date", StringType()),
        StructField("status", StringType()),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="session")
def silver_data(spark, bronze_data):
    """Silver layer — result of bronze_to_silver transformation."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))
    from datalake.layers import bronze_to_silver
    return bronze_to_silver(bronze_data)


@pytest.fixture(scope="session")
def gold_data(spark, silver_data):
    """Gold layer — result of silver_to_gold aggregation."""
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../src"))
    from datalake.layers import silver_to_gold
    return silver_to_gold(silver_data)
