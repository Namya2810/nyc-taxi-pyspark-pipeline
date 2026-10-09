"""Data-quality rules: every row is validated, and failing rows are quarantined with a reason."""
from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

MAX_ZONE_ID = 265  # TLC taxi zones are numbered 1..265 (264/265 are "Unknown")


def build_rules(month_start: date, month_end: date) -> list[tuple[str, Column]]:
    """Ordered (rule name, 'row is valid' condition). A row's reject reason is the FIRST rule it fails."""
    return [
        ("null_timestamp", F.col("tpep_pickup_datetime").isNotNull() & F.col("tpep_dropoff_datetime").isNotNull()),
        (
            "pickup_outside_month",
            (F.col("tpep_pickup_datetime") >= F.lit(month_start).cast("timestamp"))
            & (F.col("tpep_pickup_datetime") < F.lit(month_end).cast("timestamp")),
        ),
        ("dropoff_before_pickup", F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")),
        ("duration_out_of_range", F.col("trip_duration_min").between(1, 180)),
        ("invalid_distance", (F.col("trip_distance") > 0) & (F.col("trip_distance") < 100)),
        ("invalid_fare", (F.col("fare_amount") > 0) & (F.col("fare_amount") < 500)),
        ("invalid_passenger_count", F.col("passenger_count").between(1, 6)),
        (
            "unknown_location",
            F.col("PULocationID").between(1, MAX_ZONE_ID) & F.col("DOLocationID").between(1, MAX_ZONE_ID),
        ),
        ("unrealistic_speed", F.col("speed_mph") <= 80),
    ]


def tag_rejects(df: DataFrame, rules: list[tuple[str, Column]]) -> DataFrame:
    """Add a `reject_reason` column: null for valid rows, else the name of the first failed rule.

    A rule that evaluates to NULL (e.g. a missing passenger_count) counts as a failure.
    """
    reason = F.lit(None).cast("string")
    for name, valid in reversed(rules):
        reason = F.when(~F.coalesce(valid, F.lit(False)), F.lit(name)).otherwise(reason)
    return df.withColumn("reject_reason", reason)


def split_clean_rejects(tagged: DataFrame) -> tuple[DataFrame, DataFrame]:
    clean = tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejects = tagged.filter(F.col("reject_reason").isNotNull())
    return clean, rejects


def quality_report(tagged: DataFrame) -> dict:
    """Row counts overall and per rejection reason (one aggregation pass)."""
    rows = tagged.groupBy("reject_reason").count().collect()
    by_reason = {r["reject_reason"]: r["count"] for r in rows}
    clean = by_reason.pop(None, 0)
    rejected = sum(by_reason.values())
    total = clean + rejected
    return {
        "rows_in": total,
        "rows_clean": clean,
        "rows_rejected": rejected,
        "reject_rate_pct": round(100 * rejected / total, 2) if total else 0.0,
        "rejected_by_rule": dict(sorted(by_reason.items(), key=lambda kv: -kv[1])),
    }
