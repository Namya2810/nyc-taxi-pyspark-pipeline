"""Load the curated taxi data into Google BigQuery and measure what partitioning + clustering saves.

    curated parquet --> trips_flat (plain table)
                    --> trips_opt  (PARTITION BY RANGE_BUCKET(pickup_day), CLUSTER BY pickup_borough, payment_type_name)

The same filtered query is dry-run against both tables; BigQuery reports the bytes each one would scan.
Works in the free BigQuery sandbox (no billing account needed). The partition column is the integer day of month (1-31)
rather than the date itself: the sandbox expires time partitions after 60 days, which would silently delete January 2024 rows.

Run:  python -m src.bigquery_load --project YOUR_GCP_PROJECT_ID
A browser window opens once for Google sign-in consent (pydata-google-auth); no key files are created or stored in the repo.
"""
import argparse
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATASET = "nyc_taxi"
EXPORT = ROOT / "data" / "bq_export" / "trips.parquet"

# one week, one borough: the shape of query that partition pruning and clustering help most
BENCHMARK_SQL = """
    SELECT pickup_borough, COUNT(*) AS trips, ROUND(AVG(fare_amount), 2) AS avg_fare
    FROM `{table}`
    WHERE pickup_day BETWEEN 10 AND 16 AND pickup_borough = 'Manhattan'
    GROUP BY pickup_borough
"""


def export_curated() -> Path:
    """Flatten the hive-partitioned curated layer into one parquet file that includes pickup_date."""
    EXPORT.parent.mkdir(parents=True, exist_ok=True)
    glob = (ROOT / "data" / "curated" / "trips" / "**" / "*.parquet").as_posix().replace("'", "''")
    out = EXPORT.as_posix().replace("'", "''")
    duckdb.connect().execute(
        f"COPY (SELECT *, day(pickup_date) AS pickup_day FROM read_parquet('{glob}', hive_partitioning = true)) "
        f"TO '{out}' (FORMAT parquet)")
    return EXPORT


def get_client(project: str):
    import pydata_google_auth
    from google.cloud import bigquery

    credentials = pydata_google_auth.get_user_credentials(
        ["https://www.googleapis.com/auth/bigquery"], auth_local_webserver=True)
    return bigquery.Client(project=project, credentials=credentials)


def main(project: str) -> dict:
    from google.cloud import bigquery

    parquet = export_curated()
    client = get_client(project)
    client.create_dataset(bigquery.Dataset(f"{project}.{DATASET}"), exists_ok=True)
    flat, opt = f"{project}.{DATASET}.trips_flat", f"{project}.{DATASET}.trips_opt"

    with open(parquet, "rb") as f:
        job = client.load_table_from_file(
            f, flat, job_config=bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET,
                                                       write_disposition="WRITE_TRUNCATE"))
    job.result()
    rows = client.get_table(flat).num_rows
    print(f"loaded {rows:,} rows into {flat}")

    client.query(f"DROP TABLE IF EXISTS `{opt}`").result()  # BigQuery cannot replace a table with a new partition spec
    client.query(f"""CREATE TABLE `{opt}`
                     PARTITION BY RANGE_BUCKET(pickup_day, GENERATE_ARRAY(1, 32, 1))
                     CLUSTER BY pickup_borough, payment_type_name
                     AS SELECT * FROM `{flat}`""").result()
    opt_rows = client.get_table(opt).num_rows
    if opt_rows != rows:
        raise RuntimeError(f"partitioned table has {opt_rows:,} rows but the flat table has {rows:,}")

    dry = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    scanned = {name: client.query(BENCHMARK_SQL.format(table=t), job_config=dry).total_bytes_processed
               for name, t in (("flat", flat), ("partitioned_clustered", opt))}
    live = bigquery.QueryJobConfig(use_query_cache=False)
    results = {}
    for name, t in (("flat", flat), ("partitioned_clustered", opt)):
        q = client.query(BENCHMARK_SQL.format(table=t), job_config=live)
        out = list(q.result())[0]
        results[name] = {"bytes_billed": q.total_bytes_billed, "trips": out["trips"], "avg_fare": out["avg_fare"]}

    report = {
        "project_dataset": f"{project}.{DATASET}", "rows_loaded": rows,
        "benchmark_query": "days 10-16 of the month x Manhattan pickups, count and average fare",
        "bytes_scanned_dry_run": scanned,
        "bytes_scanned_reduction_pct": round(100 * (1 - scanned["partitioned_clustered"] / scanned["flat"]), 1),
        "query_results": results,
    }
    (ROOT / "reports" / "bigquery_report.json").write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps(report, indent=2, default=str))
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True, help="GCP project id (a free BigQuery sandbox project works)")
    main(parser.parse_args().project)
