"""Prefect flow that orchestrates the whole pipeline: download -> Spark ETL -> quality gate -> warehouse.

Run:  python -m src.flow --month 2024-01
"""
import argparse
from pathlib import Path

from prefect import flow, get_run_logger, task

from scripts.download_data import main as download_month
from src.pipeline import run as run_spark_etl
from src.warehouse import build_warehouse

ROOT = Path(__file__).resolve().parents[1]


def check_reject_rate(summary: dict, max_reject_rate_pct: float) -> float:
    """Raise if the share of rejected rows exceeds the allowed threshold; return the observed rate."""
    rate = summary["data_quality"]["reject_rate_pct"]
    if rate > max_reject_rate_pct:
        raise ValueError(f"reject rate {rate}% is above the allowed {max_reject_rate_pct}%: stopping before the warehouse load")
    return rate


@task(retries=2, retry_delay_seconds=5, name="download-raw-data")
def download(month: str) -> None:
    download_month(month)  # skips files that are already present


@task(name="spark-etl")
def spark_etl(month: str) -> dict:
    return run_spark_etl(month)


@task(name="quality-gate")
def quality_gate(summary: dict, max_reject_rate_pct: float) -> float:
    rate = check_reject_rate(summary, max_reject_rate_pct)
    get_run_logger().info("quality gate passed: %s%% of rows rejected (limit %s%%)", rate, max_reject_rate_pct)
    return rate


@task(retries=1, retry_delay_seconds=5, name="load-warehouse")
def load_warehouse(summary: dict) -> dict:
    return build_warehouse(ROOT / "data" / "curated" / "trips", ROOT / "data" / "raw" / "taxi_zone_lookup.csv",
                           ROOT / "data" / "warehouse" / "taxi.duckdb", ROOT / "reports", summary)


@flow(name="nyc-taxi-pipeline", log_prints=True)
def taxi_pipeline(month: str = "2024-01", max_reject_rate_pct: float = 15.0) -> dict:
    download(month)
    summary = spark_etl(month)
    quality_gate(summary, max_reject_rate_pct)
    warehouse = load_warehouse(summary)
    print(f"done: {summary['curated_rows']} curated rows, warehouse tables {warehouse['table_rows']}")
    return {"etl": summary["total_seconds"], "warehouse_load": warehouse["load_seconds"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", default="2024-01")
    parser.add_argument("--max-reject-rate", type=float, default=15.0)
    args = parser.parse_args()
    taxi_pipeline(args.month, args.max_reject_rate)
