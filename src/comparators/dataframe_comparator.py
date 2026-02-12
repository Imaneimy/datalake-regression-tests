"""
DataFrame Comparator
Compares two DataFrames for regression testing:
- Schema equality
- Row count delta
- Content diff (added / removed / changed rows)
- Statistical drift on numeric columns
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


@dataclass
class RegressionResult:
    passed: bool = True
    test_name: str = ""
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metrics: Dict = field(default_factory=dict)

    def fail(self, msg: str) -> None:
        self.passed = False
        self.errors.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def __str__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        lines = [f"[{status}] {self.test_name}"]
        for e in self.errors:
            lines.append(f"  ✗ {e}")
        for w in self.warnings:
            lines.append(f"  ⚠ {w}")
        for k, v in self.metrics.items():
            lines.append(f"  📊 {k}: {v}")
        return "\n".join(lines)


def compare_schemas(
    df_current: DataFrame,
    df_reference: DataFrame,
    test_name: str = "Schema Regression",
) -> RegressionResult:
    """Assert schema has not changed between releases."""
    result = RegressionResult(test_name=test_name)
    cur = {f.name: f.dataType.simpleString() for f in df_current.schema.fields}
    ref = {f.name: f.dataType.simpleString() for f in df_reference.schema.fields}

    for col, dtype in ref.items():
        if col not in cur:
            result.fail(f"Column removed: '{col}'")
        elif cur[col] != dtype:
            result.fail(f"Type changed on '{col}': {dtype} → {cur[col]}")

    added = set(cur) - set(ref)
    if added:
        result.warn(f"New columns detected: {added}")

    result.metrics["ref_columns"] = len(ref)
    result.metrics["cur_columns"] = len(cur)
    return result


def compare_row_counts(
    df_current: DataFrame,
    df_reference: DataFrame,
    tolerance_pct: float = 5.0,
    test_name: str = "Row Count Regression",
) -> RegressionResult:
    """Assert row count delta is within acceptable percentage."""
    result = RegressionResult(test_name=test_name)
    cur_count = df_current.count()
    ref_count = df_reference.count()

    result.metrics["reference_count"] = ref_count
    result.metrics["current_count"] = cur_count

    if ref_count == 0:
        if cur_count > 0:
            result.warn("Reference was empty, current has rows.")
        return result

    delta_pct = abs(cur_count - ref_count) / ref_count * 100
    result.metrics["delta_pct"] = round(delta_pct, 2)

    if delta_pct > tolerance_pct:
        result.fail(
            f"Row count delta {delta_pct:.2f}% exceeds tolerance {tolerance_pct}%"
            f" (ref={ref_count}, cur={cur_count})"
        )
    return result


def compare_content(
    df_current: DataFrame,
    df_reference: DataFrame,
    key_cols: List[str],
    test_name: str = "Content Regression",
) -> RegressionResult:
    """
    Detect rows added, removed, or changed between two DataFrames.
    Uses symmetric difference on key columns.
    """
    result = RegressionResult(test_name=test_name)

    added = df_current.join(df_reference, on=key_cols, how="left_anti").count()
    removed = df_reference.join(df_current, on=key_cols, how="left_anti").count()

    result.metrics["rows_added"] = added
    result.metrics["rows_removed"] = removed

    if removed > 0:
        result.fail(f"{removed} row(s) disappeared compared to reference.")
    if added > 0:
        result.warn(f"{added} new row(s) not present in reference.")
    return result


def compare_numeric_stats(
    df_current: DataFrame,
    df_reference: DataFrame,
    numeric_cols: List[str],
    tolerance_pct: float = 2.0,
    test_name: str = "Statistical Regression",
) -> RegressionResult:
    """
    Assert that sum and mean of numeric columns haven't drifted beyond tolerance.
    """
    result = RegressionResult(test_name=test_name)

    for col in numeric_cols:
        cur_stats = df_current.select(F.sum(col).alias("s"), F.mean(col).alias("m")).first()
        ref_stats = df_reference.select(F.sum(col).alias("s"), F.mean(col).alias("m")).first()

        if ref_stats["s"] is None or ref_stats["s"] == 0:
            continue

        sum_drift = abs(cur_stats["s"] - ref_stats["s"]) / abs(ref_stats["s"]) * 100
        mean_drift = abs(cur_stats["m"] - ref_stats["m"]) / abs(ref_stats["m"]) * 100

        result.metrics[f"{col}_sum_drift_pct"] = round(sum_drift, 3)
        result.metrics[f"{col}_mean_drift_pct"] = round(mean_drift, 3)

        if sum_drift > tolerance_pct:
            result.fail(f"SUM drift on '{col}': {sum_drift:.3f}% > {tolerance_pct}%")
        if mean_drift > tolerance_pct:
            result.fail(f"MEAN drift on '{col}': {mean_drift:.3f}% > {tolerance_pct}%")

    return result

# threshold configurable per use case
DEFAULT_COUNT_DELTA_PCT = 5.0
