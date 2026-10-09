"""Column derivations and the zone-lookup join."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

PAYMENT_TYPES = {1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided trip"}


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Duration, speed and calendar columns (computed before validation so rules can use them)."""
    duration_min = (
        F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")
    ) / 60.0
    return (
        # the TLC files use timestamp_ntz; normalise to timestamp (session time zone is UTC)
        df.withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("trip_duration_min", F.round(duration_min, 2))
        .withColumn(
            "speed_mph",
            F.when(F.col("trip_duration_min") > 0, F.col("trip_distance") / (F.col("trip_duration_min") / 60.0)),
        )
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    )


def enrich(df: DataFrame, zones: DataFrame) -> DataFrame:
    """Join pickup/dropoff zone names (small lookup table, so it is broadcast) and add tip %."""
    pu = F.broadcast(zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
    ))
    do = F.broadcast(zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
    ))
    payment_name = F.create_map(*[x for k, v in PAYMENT_TYPES.items() for x in (F.lit(k), F.lit(v))])[F.col("payment_type")]
    return (
        df.join(pu, "PULocationID", "left")
        .join(do, "DOLocationID", "left")
        .withColumn("payment_type_name", F.coalesce(payment_name, F.lit("Unknown")))
        # tips are only recorded for card payments, so tip % is only meaningful there
        .withColumn(
            "tip_pct",
            F.when(
                (F.col("payment_type") == 1) & (F.col("fare_amount") > 0),
                F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2),
            ),
        )
    )
