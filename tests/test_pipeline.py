from datetime import date, datetime

from pyspark.sql.types import (DoubleType, IntegerType, LongType, StructField, StructType,
                               TimestampNTZType)

from src.pipeline import month_bounds
from src.quality import build_rules, quality_report, split_clean_rejects, tag_rejects
from src.transform import add_derived_columns, enrich

# same column types as the real TLC parquet files (timestamp_ntz, long passenger_count, ...)
SCHEMA = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType()),
    StructField("tpep_dropoff_datetime", TimestampNTZType()),
    StructField("passenger_count", LongType()),
    StructField("trip_distance", DoubleType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", LongType()),
    StructField("fare_amount", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("total_amount", DoubleType()),
])
COLUMNS = SCHEMA.fieldNames()


def trips(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


GOOD = (datetime(2024, 1, 5, 8, 0), datetime(2024, 1, 5, 8, 20), 2, 5.0, 132, 161, 1, 20.0, 5.0, 28.0)


def reasons(spark, rows):
    rules = build_rules(date(2024, 1, 1), date(2024, 2, 1))
    tagged = tag_rejects(add_derived_columns(trips(spark, rows)), rules)
    return [r["reject_reason"] for r in tagged.orderBy("fare_amount", "trip_distance").collect()]


def test_month_bounds_handles_december():
    assert month_bounds("2024-12") == (date(2024, 12, 1), date(2025, 1, 1))
    assert month_bounds("2024-01") == (date(2024, 1, 1), date(2024, 2, 1))


def test_valid_row_passes(spark):
    assert reasons(spark, [GOOD]) == [None]


def test_each_rule_rejects_its_row(spark):
    base = list(GOOD)

    def variant(**changes):
        row = dict(zip(COLUMNS, base))
        row.update(changes)
        return tuple(row[c] for c in COLUMNS)

    cases = {
        "dropoff_before_pickup": variant(tpep_dropoff_datetime=datetime(2024, 1, 5, 7, 0)),
        "pickup_outside_month": variant(
            tpep_pickup_datetime=datetime(2023, 12, 31, 8, 0), tpep_dropoff_datetime=datetime(2023, 12, 31, 8, 20)),
        "invalid_distance": variant(trip_distance=0.0),
        "invalid_fare": variant(fare_amount=-3.0),
        "invalid_passenger_count": variant(passenger_count=None),
        "unknown_location": variant(PULocationID=0),
        "duration_out_of_range": variant(tpep_dropoff_datetime=datetime(2024, 1, 5, 8, 0, 20)),
    }
    for expected, row in cases.items():
        assert reasons(spark, [row]) == [expected], expected


def test_first_failing_rule_wins(spark):
    # zero distance AND negative fare: invalid_distance comes first in the rule order
    row = (GOOD[0], GOOD[1], 2, 0.0, 132, 161, 1, -3.0, 0.0, 0.0)
    assert reasons(spark, [row]) == ["invalid_distance"]


def test_unrealistic_speed_is_rejected(spark):
    # 100 miles in 90 minutes is ~67 mph (kept); 120 miles in 90 minutes is 80+ mph (but distance < 100 rule)
    fast = (GOOD[0], datetime(2024, 1, 5, 8, 30), 2, 90.0, 132, 161, 1, 20.0, 5.0, 28.0)
    assert reasons(spark, [fast]) == ["unrealistic_speed"]


def test_quality_report_counts_add_up(spark):
    bad = (GOOD[0], GOOD[1], None, 5.0, 132, 161, 1, 20.0, 5.0, 28.0)
    rules = build_rules(date(2024, 1, 1), date(2024, 2, 1))
    tagged = tag_rejects(add_derived_columns(trips(spark, [GOOD, GOOD, bad])), rules)
    report = quality_report(tagged)
    clean, rejects = split_clean_rejects(tagged)
    assert report["rows_in"] == 3 and report["rows_clean"] == 2 and report["rows_rejected"] == 1
    assert report["rejected_by_rule"] == {"invalid_passenger_count": 1}
    assert clean.count() == 2 and rejects.count() == 1


def test_derived_columns(spark):
    row = add_derived_columns(trips(spark, [GOOD])).collect()[0]
    assert row["trip_duration_min"] == 20.0
    assert round(row["speed_mph"], 2) == 15.0
    assert row["pickup_date"] == date(2024, 1, 5) and row["pickup_hour"] == 8


def test_enrich_keeps_row_count_and_adds_zone_names_and_tips(spark):
    zones = spark.createDataFrame(
        [(132, "Queens", "JFK Airport"), (161, "Manhattan", "Midtown Center")], ["LocationID", "Borough", "Zone"])
    cash = (GOOD[0], GOOD[1], 1, 2.0, 132, 161, 2, 10.0, 0.0, 12.0)
    out = enrich(add_derived_columns(trips(spark, [GOOD, cash])), zones)
    assert out.count() == 2
    by_fare = {r["fare_amount"]: r for r in out.collect()}
    assert by_fare[20.0]["pickup_zone"] == "JFK Airport" and by_fare[20.0]["dropoff_borough"] == "Manhattan"
    assert by_fare[20.0]["tip_pct"] == 25.0 and by_fare[20.0]["payment_type_name"] == "Credit card"
    assert by_fare[10.0]["tip_pct"] is None and by_fare[10.0]["payment_type_name"] == "Cash"
