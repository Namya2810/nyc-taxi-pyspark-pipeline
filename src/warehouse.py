"""Load the curated parquet layer into a star-schema warehouse (DuckDB) and run analytical SQL.

    raw parquet --Spark ETL--> curated parquet (data lake layer) --SQL load--> star schema (warehouse)

Run (after the Spark pipeline):  python -m src.warehouse
"""
import json
import logging
import time
from pathlib import Path

import duckdb

from src.transform import PAYMENT_TYPES

ROOT = Path(__file__).resolve().parents[1]
log = logging.getLogger("taxi_warehouse")

# ----------------------------------------------------------------------------- schema
SCHEMA = """
DROP TABLE IF EXISTS fact_trips;
DROP TABLE IF EXISTS dim_zone;
DROP TABLE IF EXISTS dim_payment_type;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_zone (
    zone_key     INTEGER PRIMARY KEY,   -- TLC LocationID
    borough      VARCHAR,
    zone         VARCHAR,
    service_zone VARCHAR
);
CREATE TABLE dim_payment_type (
    payment_type_key INTEGER PRIMARY KEY,
    payment_type     VARCHAR
);
CREATE TABLE dim_date (
    date_key    INTEGER PRIMARY KEY,    -- yyyymmdd
    full_date   DATE,
    year        INTEGER,
    month       INTEGER,
    day         INTEGER,
    day_of_week VARCHAR,
    is_weekend  BOOLEAN
);
CREATE TABLE fact_trips (
    pickup_date_key   INTEGER  REFERENCES dim_date (date_key),
    pickup_hour       INTEGER,
    pickup_zone_key   INTEGER  REFERENCES dim_zone (zone_key),
    dropoff_zone_key  INTEGER  REFERENCES dim_zone (zone_key),
    payment_type_key  INTEGER  REFERENCES dim_payment_type (payment_type_key),
    passenger_count   INTEGER,
    trip_distance     DOUBLE,
    trip_duration_min DOUBLE,
    speed_mph         DOUBLE,
    fare_amount       DOUBLE,
    tip_amount        DOUBLE,
    total_amount      DOUBLE,
    tip_pct           DOUBLE
);
CREATE TABLE IF NOT EXISTS etl_run_log (
    month VARCHAR, run_at_utc VARCHAR, rows_in BIGINT, rows_clean BIGINT, rows_rejected BIGINT,
    reject_rate_pct DOUBLE, total_seconds DOUBLE
);
CREATE TABLE IF NOT EXISTS dq_rejects_by_rule (month VARCHAR, rule VARCHAR, rows BIGINT);
"""

# ----------------------------------------------------------------------------- analytics
QUERIES = {
    "revenue_by_borough_weekday": """
        SELECT z.borough, d.day_of_week,
               COUNT(*)                        AS trips,
               ROUND(SUM(f.total_amount), 2)   AS revenue,
               ROUND(AVG(f.fare_amount), 2)    AS avg_fare
        FROM fact_trips f
        JOIN dim_zone z ON z.zone_key = f.pickup_zone_key
        JOIN dim_date d ON d.date_key = f.pickup_date_key
        WHERE z.borough NOT IN ('Unknown', 'N/A')
        GROUP BY z.borough, d.day_of_week
        ORDER BY z.borough, revenue DESC
    """,
    # window function: the 3 busiest pickup->dropoff routes inside each pickup borough
    "top_routes_per_borough": """
        SELECT pickup_borough, pickup_zone, dropoff_zone, trips, avg_fare, avg_miles
        FROM (
            SELECT pz.borough AS pickup_borough, pz.zone AS pickup_zone, dz.zone AS dropoff_zone,
                   COUNT(*)                      AS trips,
                   ROUND(AVG(f.fare_amount), 2)  AS avg_fare,
                   ROUND(AVG(f.trip_distance), 2) AS avg_miles,
                   ROW_NUMBER() OVER (PARTITION BY pz.borough ORDER BY COUNT(*) DESC) AS rn
            FROM fact_trips f
            JOIN dim_zone pz ON pz.zone_key = f.pickup_zone_key
            JOIN dim_zone dz ON dz.zone_key = f.dropoff_zone_key
            WHERE pz.borough NOT IN ('Unknown', 'N/A')
            GROUP BY pz.borough, pz.zone, dz.zone
        )
        WHERE rn <= 3
        ORDER BY pickup_borough, trips DESC
    """,
    # window function: 7-day moving average of daily trips
    "daily_trips_moving_avg": """
        SELECT full_date, trips, ROUND(AVG(trips) OVER (ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) AS trips_7d_avg
        FROM (
            SELECT d.full_date, COUNT(*) AS trips
            FROM fact_trips f JOIN dim_date d ON d.date_key = f.pickup_date_key
            GROUP BY d.full_date
        )
        ORDER BY full_date
    """,
    "airport_revenue_share": """
        SELECT d.is_weekend,
               ROUND(100.0 * SUM(CASE WHEN z.zone LIKE '%Airport%' THEN f.total_amount END) / SUM(f.total_amount), 2) AS airport_revenue_pct,
               COUNT(*) AS trips
        FROM fact_trips f
        JOIN dim_zone z ON z.zone_key = f.pickup_zone_key
        JOIN dim_date d ON d.date_key = f.pickup_date_key
        GROUP BY d.is_weekend
        ORDER BY d.is_weekend
    """,
    "tip_by_pickup_borough": """
        SELECT z.borough, COUNT(*) AS card_trips, ROUND(AVG(f.tip_pct), 2) AS avg_tip_pct
        FROM fact_trips f
        JOIN dim_zone z ON z.zone_key = f.pickup_zone_key
        JOIN dim_payment_type p ON p.payment_type_key = f.payment_type_key
        WHERE p.payment_type = 'Credit card' AND z.borough NOT IN ('Unknown', 'N/A')
        GROUP BY z.borough
        ORDER BY avg_tip_pct DESC
    """,
}


def _posix(path: Path) -> str:
    return Path(path).as_posix()


def load(con: duckdb.DuckDBPyConnection, curated_dir: Path, zones_csv: Path) -> None:
    con.execute(SCHEMA)
    glob = _posix(Path(curated_dir) / "**" / "*.parquet").replace("'", "''")  # views cannot take bound parameters
    con.execute(f"CREATE OR REPLACE TEMP VIEW stg_trips AS SELECT * FROM read_parquet('{glob}', hive_partitioning = true)")
    con.execute(
        "INSERT INTO dim_zone SELECT LocationID, Borough, Zone, service_zone FROM read_csv_auto(?, header = true)",
        [_posix(zones_csv)],
    )
    names = ", ".join(f"({k}, '{v}')" for k, v in PAYMENT_TYPES.items())
    con.execute(
        f"""INSERT INTO dim_payment_type
            SELECT s.payment_type, COALESCE(m.name, 'Unknown')
            FROM (SELECT DISTINCT payment_type FROM stg_trips WHERE payment_type IS NOT NULL) s
            LEFT JOIN (VALUES {names}) AS m(id, name) ON m.id = s.payment_type"""
    )
    con.execute(
        """INSERT INTO dim_date
           SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER), CAST(d AS DATE), year(d), month(d), day(d),
                  dayname(d), isodow(d) IN (6, 7)
           FROM (SELECT UNNEST(generate_series(
                    (SELECT MIN(pickup_date) FROM stg_trips)::TIMESTAMP,
                    (SELECT MAX(pickup_date) FROM stg_trips)::TIMESTAMP, INTERVAL 1 DAY)) AS d)"""
    )
    con.execute(
        """INSERT INTO fact_trips
           SELECT CAST(strftime(pickup_date, '%Y%m%d') AS INTEGER), pickup_hour, PULocationID, DOLocationID,
                  payment_type, passenger_count, trip_distance, trip_duration_min, speed_mph,
                  fare_amount, tip_amount, total_amount, tip_pct
           FROM stg_trips"""
    )


def check_integrity(con: duckdb.DuckDBPyConnection) -> dict:
    """Reconcile the warehouse with the curated layer; raise if anything is off."""
    one = lambda sql: con.execute(sql).fetchone()[0]
    checks = {
        "fact_rows_equal_curated_rows": one("SELECT (SELECT COUNT(*) FROM fact_trips) = (SELECT COUNT(*) FROM stg_trips)"),
        "total_amount_matches_curated": one(
            "SELECT ABS((SELECT SUM(total_amount) FROM fact_trips) - (SELECT SUM(total_amount) FROM stg_trips)) < 0.01"),
        "no_null_keys": one(
            """SELECT COUNT(*) = 0 FROM fact_trips WHERE pickup_date_key IS NULL OR pickup_zone_key IS NULL
               OR dropoff_zone_key IS NULL OR payment_type_key IS NULL"""),
        "no_orphan_zone_keys": one(
            """SELECT COUNT(*) = 0 FROM fact_trips f
               LEFT JOIN dim_zone p ON p.zone_key = f.pickup_zone_key
               LEFT JOIN dim_zone d ON d.zone_key = f.dropoff_zone_key
               WHERE p.zone_key IS NULL OR d.zone_key IS NULL"""),
        "no_orphan_date_or_payment_keys": one(
            """SELECT COUNT(*) = 0 FROM fact_trips f
               LEFT JOIN dim_date d ON d.date_key = f.pickup_date_key
               LEFT JOIN dim_payment_type p ON p.payment_type_key = f.payment_type_key
               WHERE d.date_key IS NULL OR p.payment_type_key IS NULL"""),
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise RuntimeError(f"warehouse integrity checks failed: {failed}")
    return checks


def log_run(con: duckdb.DuckDBPyConnection, run_report: dict) -> None:
    """Keep the Spark run's data-quality metrics in the warehouse so they can be queried over time."""
    dq = run_report["data_quality"]
    con.execute("DELETE FROM etl_run_log WHERE month = ?", [run_report["month"]])
    con.execute("DELETE FROM dq_rejects_by_rule WHERE month = ?", [run_report["month"]])
    con.execute(
        "INSERT INTO etl_run_log VALUES (?, ?, ?, ?, ?, ?, ?)",
        [run_report["month"], run_report["run_at_utc"], dq["rows_in"], dq["rows_clean"], dq["rows_rejected"],
         dq["reject_rate_pct"], run_report["total_seconds"]],
    )
    for rule, n in dq["rejected_by_rule"].items():
        con.execute("INSERT INTO dq_rejects_by_rule VALUES (?, ?, ?)", [run_report["month"], rule, n])


def build_warehouse(curated_dir: Path, zones_csv: Path, db_path: Path, reports_dir: Path,
                    run_report: dict | None = None) -> dict:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    reports_dir = Path(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    t0 = time.perf_counter()
    load(con, curated_dir, zones_csv)
    load_seconds = round(time.perf_counter() - t0, 2)
    checks = check_integrity(con)
    if run_report:
        log_run(con, run_report)

    tables = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
              for t in ("fact_trips", "dim_zone", "dim_payment_type", "dim_date", "etl_run_log", "dq_rejects_by_rule")}
    query_seconds = {}
    for name, sql in QUERIES.items():
        t0 = time.perf_counter()
        frame = con.execute(sql).fetchdf()
        query_seconds[name] = round(time.perf_counter() - t0, 3)
        frame.to_csv(reports_dir / f"wh_{name}.csv", index=False)
        log.info("query %-28s %5d rows in %.3fs", name, len(frame), query_seconds[name])
    con.close()

    report = {"load_seconds": load_seconds, "table_rows": tables, "integrity_checks": checks,
              "query_seconds": query_seconds}
    (reports_dir / "warehouse_report.json").write_text(json.dumps(report, indent=2))
    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    reports = ROOT / "reports"
    summary = json.loads((reports / "run_report.json").read_text())
    result = build_warehouse(ROOT / "data" / "curated" / "trips", ROOT / "data" / "raw" / "taxi_zone_lookup.csv",
                             ROOT / "data" / "warehouse" / "taxi.duckdb", reports, summary)
    log.info("warehouse built in %.2fs: %s", result["load_seconds"], result["table_rows"])
    log.info("integrity checks: %s", result["integrity_checks"])
