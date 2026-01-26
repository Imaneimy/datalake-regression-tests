"""
Datalake Layer Definitions — Bronze / Silver / Gold
Simulates Medallion Architecture transformations.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import logging

logger = logging.getLogger(__name__)

# ── Schema definitions ──────────────────────────────────────────────────────

BRONZE_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", StringType(), True),      # raw — may contain garbage
    StructField("event_date", StringType(), True),
    StructField("status", StringType(), True),
])

SILVER_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("event_date", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("_ingestion_ts", TimestampType(), True),
])

GOLD_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("transaction_count", StringType(), False),
    StructField("avg_amount", DoubleType(), False),
    StructField("first_event", TimestampType(), True),
    StructField("last_event", TimestampType(), True),
])


# ── Layer transformations ────────────────────────────────────────────────────

def bronze_to_silver(df_bronze: DataFrame) -> DataFrame:
    """
    Raw → Cleaned:
    - Drop nulls on key columns
    - Cast amount to double
    - Parse event_date to timestamp
    - Filter invalid statuses
    - Add audit column
    """
    return (
        df_bronze
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("amount").isNotNull())
        .filter(F.col("event_date").isNotNull())
        .withColumn("amount", F.col("amount").cast(DoubleType()))
        .filter(F.col("amount") > 0)
        .withColumn("event_date", F.to_timestamp("event_date", "yyyy-MM-dd"))
        .filter(F.col("status").isin(["COMPLETED", "PENDING", "REFUNDED"]))
        .withColumn("category", F.upper(F.trim(F.col("category"))))
        .withColumn("_ingestion_ts", F.current_timestamp())
        .dropDuplicates(["event_id"])
    )


def silver_to_gold(df_silver: DataFrame) -> DataFrame:
    """
    Cleaned → Aggregated:
    Customer spending summary per category.
    Only COMPLETED transactions are counted.
    """
    return (
        df_silver
        .filter(F.col("status") == "COMPLETED")
        .groupBy("customer_id", "category")
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("*").cast(StringType()).alias("transaction_count"),
            F.avg("amount").alias("avg_amount"),
            F.min("event_date").alias("first_event"),
            F.max("event_date").alias("last_event"),
        )
        .orderBy("customer_id", "category")
    )
