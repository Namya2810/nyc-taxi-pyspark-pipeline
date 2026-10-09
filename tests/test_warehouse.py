import json
from datetime import date

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from src.warehouse import build_warehouse

ZONES = "LocationID,Borough,Zone,service_zone\n132,Queens,JFK Airport,Airports\n161,Manhattan,Midtown Center,Yellow Zone\n"

RUN_REPORT = {
    "month": "2024-01", "run_at_utc": "2024-02-01 00:00:00", "total_seconds": 1.5,
    "data_quality": {"rows_in": 5, "rows_clean": 3, "rows_rejected": 2, "reject_rate_pct": 40.0,
                     "rejected_by_rule": {"invalid_fare": 2}},
}


def curated(tmp_path, rows):
    cols = ["PULocationID", "DOLocationID", "payment_type", "passenger_count", "trip_distance", "trip_duration_min",
            "speed_mph", "pickup_hour", "fare_amount", "tip_amount", "total_amount", "tip_pct", "pickup_date"]
    out = tmp_path / "curated"
    ds.write_dataset(pa.Table.from_pandas(pd.DataFrame(rows, columns=cols)), str(out), format="parquet",
                     partitioning=["pickup_date"], partitioning_flavor="hive")
    zones = tmp_path / "zones.csv"
    zones.write_text(ZONES)
    return out, zones


GOOD = [
    (132, 161, 1, 2, 10.0, 30.0, 20.0, 8, 50.0, 10.0, 62.0, 20.0, date(2024, 1, 5)),   # card, airport pickup
    (161, 161, 2, 1, 1.0, 10.0, 6.0, 9, 8.0, 0.0, 10.0, None, date(2024, 1, 5)),       # cash
    (161, 132, 1, 1, 8.0, 25.0, 19.0, 10, 30.0, 6.0, 38.0, 20.0, date(2024, 1, 6)),    # card, weekend
]


def test_star_schema_is_loaded_and_reconciled(tmp_path):
    out, zones = curated(tmp_path, GOOD)
    report = build_warehouse(out, zones, tmp_path / "wh.duckdb", tmp_path / "reports", RUN_REPORT)
    assert report["table_rows"]["fact_trips"] == 3
    assert report["table_rows"]["dim_zone"] == 2
    assert report["table_rows"]["dim_date"] == 2          # Jan 5 and Jan 6
    assert report["table_rows"]["dim_payment_type"] == 2  # card and cash
    assert all(report["integrity_checks"].values())


def test_dimensions_carry_the_right_attributes(tmp_path):
    out, zones = curated(tmp_path, GOOD)
    build_warehouse(out, zones, tmp_path / "wh.duckdb", tmp_path / "reports")
    con = duckdb.connect(str(tmp_path / "wh.duckdb"))
    days = dict(con.execute("SELECT full_date, is_weekend FROM dim_date").fetchall())
    assert days[date(2024, 1, 5)] is False and days[date(2024, 1, 6)] is True   # Fri vs Sat
    pay = dict(con.execute("SELECT payment_type_key, payment_type FROM dim_payment_type").fetchall())
    assert pay == {1: "Credit card", 2: "Cash"}


def test_analytical_queries_run_and_aggregate_correctly(tmp_path):
    out, zones = curated(tmp_path, GOOD)
    build_warehouse(out, zones, tmp_path / "wh.duckdb", tmp_path / "reports")
    airport = pd.read_csv(tmp_path / "reports" / "wh_airport_revenue_share.csv").set_index("is_weekend")
    assert airport.loc[False, "trips"] == 2 and airport.loc[True, "trips"] == 1
    # weekday: airport fare 62 of 72 total; weekend: 0 of 38
    assert airport.loc[False, "airport_revenue_pct"] == pytest.approx(100 * 62 / 72, abs=0.01)
    assert airport.loc[True, "airport_revenue_pct"] == 0 or pd.isna(airport.loc[True, "airport_revenue_pct"])


def test_run_metrics_are_logged_in_the_warehouse(tmp_path):
    out, zones = curated(tmp_path, GOOD)
    build_warehouse(out, zones, tmp_path / "wh.duckdb", tmp_path / "reports", RUN_REPORT)
    con = duckdb.connect(str(tmp_path / "wh.duckdb"))
    assert con.execute("SELECT rows_in, rows_rejected FROM etl_run_log").fetchone() == (5, 2)
    assert con.execute("SELECT rule, rows FROM dq_rejects_by_rule").fetchall() == [("invalid_fare", 2)]
    # rebuilding the same month replaces its rows instead of duplicating them
    build_warehouse(out, zones, tmp_path / "wh.duckdb", tmp_path / "reports", RUN_REPORT)
    assert con.execute("SELECT COUNT(*) FROM etl_run_log").fetchone()[0] == 1


def test_unknown_zone_is_rejected_by_the_foreign_key(tmp_path):
    bad = GOOD + [(999, 161, 1, 1, 2.0, 5.0, 24.0, 7, 9.0, 1.0, 11.0, 11.0, date(2024, 1, 5))]
    out, zones = curated(tmp_path, bad)
    with pytest.raises(duckdb.Error):
        build_warehouse(out, zones, tmp_path / "wh.duckdb", tmp_path / "reports")
