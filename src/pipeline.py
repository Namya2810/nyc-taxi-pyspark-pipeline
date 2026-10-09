"""NYC taxi ETL: extract raw parquet -> validate/quarantine -> enrich -> curated parquet -> SQL reports.

Run:  python -m src.pipeline --month 2024-01
"""
import argparse
import json
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path

from pyspark import StorageLevel

from src.analytics import run_reports
from src.quality import build_rules, quality_report, split_clean_rejects, tag_rejects
from src.spark_session import get_spark, read_parquet, write_parquet
from src.transform import add_derived_columns, enrich

ROOT = Path(__file__).resolve().parents[1]
log = logging.getLogger("taxi_pipeline")


def month_bounds(month: str) -> tuple[date, date]:
    start = datetime.strptime(month, "%Y-%m").date()
    end = date(start.year + (start.month == 12), start.month % 12 + 1, 1)
    return start, end


def setup_logging() -> None:
    (ROOT / "logs").mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(ROOT / "logs" / "pipeline.log", mode="w"), logging.StreamHandler()],
    )


def run(month: str) -> dict:
    setup_logging()
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    raw_dir, data_dir, reports_dir = ROOT / "data" / "raw", ROOT / "data", ROOT / "reports"
    reports_dir.mkdir(exist_ok=True)
    month_start, month_end = month_bounds(month)
    timings: dict[str, float] = {}

    def stage(name: str, t0: float) -> None:
        timings[name] = round(time.perf_counter() - t0, 2)
        log.info("stage %-10s done in %.2fs", name, timings[name])

    # ---- extract
    t0 = time.perf_counter()
    raw = spark.read.parquet(str(raw_dir / f"yellow_tripdata_{month}.parquet"))
    zones = spark.read.option("header", True).option("inferSchema", True).csv(str(raw_dir / "taxi_zone_lookup.csv"))
    log.info("extract: %d input partitions, %d zones", raw.rdd.getNumPartitions(), zones.count())
    stage("extract", t0)

    # ---- validate: tag every row, quarantine the failures
    t0 = time.perf_counter()
    tagged = tag_rejects(add_derived_columns(raw), build_rules(month_start, month_end)).persist(StorageLevel.MEMORY_AND_DISK)
    quality = quality_report(tagged)
    log.info("validate: %d in, %d clean, %d rejected (%.2f%%)", quality["rows_in"], quality["rows_clean"],
             quality["rows_rejected"], quality["reject_rate_pct"])
    for rule, n in quality["rejected_by_rule"].items():
        log.info("  rejected by %-24s %d", rule, n)
    stage("validate", t0)

    # ---- transform + load curated layer
    t0 = time.perf_counter()
    clean, rejects = split_clean_rejects(tagged)
    curated = enrich(clean, zones)
    write_parquet(curated, data_dir / "curated" / "trips", ["pickup_date"])
    write_parquet(rejects, data_dir / "quarantine" / "rejects")
    tagged.unpersist()
    stage("load", t0)

    # ---- reconciliation gate: the join must not add or drop rows
    curated_rows = read_parquet(spark, data_dir / "curated" / "trips").count()
    if curated_rows != quality["rows_clean"]:
        raise RuntimeError(f"row count mismatch after load: {curated_rows} curated vs {quality['rows_clean']} clean")
    log.info("reconciliation ok: %d curated rows == %d clean rows", curated_rows, quality["rows_clean"])

    # ---- reports from the curated layer (Spark SQL)
    t0 = time.perf_counter()
    reports = run_reports(spark, read_parquet(spark, data_dir / "curated" / "trips"))
    for name, frame in reports.items():
        frame.toPandas().to_csv(reports_dir / f"{name}.csv", index=False)
        log.info("report written: %s.csv", name)
    stage("report", t0)

    summary = {
        "month": month,
        "run_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "spark_version": spark.version,
        "data_quality": quality,
        "curated_rows": curated_rows,
        "stage_seconds": timings,
        "total_seconds": round(sum(timings.values()), 2),
    }
    (reports_dir / "run_report.json").write_text(json.dumps(summary, indent=2))
    log.info("pipeline finished in %.2fs", summary["total_seconds"])
    spark.stop()
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", default="2024-01", help="YYYY-MM of the raw file in data/raw")
    run(parser.parse_args().month)
