# NYC Taxi PySpark ETL Pipeline

A batch data pipeline built with **PySpark** that ingests one month of NYC yellow-taxi trips (~3 million rows),
validates every record against data-quality rules, quarantines the bad rows with a reason, enriches the good ones,
writes a partitioned **curated parquet layer**, and produces **Spark SQL** analytics reports. A second stage loads the
curated layer into a **star-schema warehouse (DuckDB)** with integrity checks and window-function analytics.

Data: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), January 2024
(`yellow_tripdata_2024-01.parquet`, 2,964,624 rows) plus the taxi-zone lookup table (265 zones).

## Pipeline

```
data/raw  ──►  extract  ──►  derive + validate  ──►  enrich (zone join)  ──►  data/curated/trips   (parquet, partitioned by pickup_date)
 parquet          │              │   └── failing rows ─────────────────────►  data/quarantine/rejects (parquet, with reject_reason)
 + zones.csv      │              └── run metrics ──────────────────────────►  reports/run_report.json
                  └─────────────────────────────  curated layer ──► Spark SQL ──► reports/*.csv
                                                   curated layer ──► star schema (DuckDB) ──► reports/wh_*.csv
```

| Stage | What it does |
|---|---|
| **Extract** | Reads the raw parquet and the zone-lookup CSV |
| **Validate** | Derives trip duration and speed, then applies 9 ordered rules; each failing row is tagged with the *first* rule it breaks. NULLs count as failures |
| **Load** | Joins pickup/dropoff zone names (broadcast join of the small lookup table), computes tip %, writes the curated layer partitioned by `pickup_date` and the quarantine layer |
| **Reconcile** | A gate that fails the run if curated rows != clean rows (the join must not add or drop rows) |
| **Report** | Reads the curated layer back and runs Spark SQL: daily summary, hourly demand by borough, top zones per borough (window function), payment type and tips |

Each run logs every stage to `logs/pipeline.log` and writes timings and quality counts to `reports/run_report.json`.

## Results (January 2024, local run)

| Metric | Value |
|---|---|
| Rows in | 2,964,624 |
| Clean rows (curated) | 2,713,337 |
| Rejected rows (quarantined) | 251,287 (8.48%) |
| Total runtime | 56.7 s (extract 6.0, validate 9.4, load 33.4, report 8.0) |

Rows rejected, by rule:

| Rule | Rows |
|---|---|
| invalid_passenger_count (missing or outside 1-6) | 145,520 |
| invalid_distance (<= 0 or >= 100 miles) | 36,269 |
| duration_out_of_range (outside 1-180 min) | 36,234 |
| invalid_fare (<= 0 or >= 500) | 32,294 |
| dropoff_before_pickup | 870 |
| unrealistic_speed (> 80 mph) | 82 |
| pickup_outside_month | 18 |

A few findings from the curated data (full tables in `reports/`):
- Busiest pickup zones: JFK Airport (135,476 trips, average fare $62.96), Upper East Side South (135,167) and Midtown Center (134,651).
- Card payments (2,268,552 trips) average a **25.64% tip**; cash tips are not recorded, so tip % is only computed for card trips.

## Warehouse layer (star schema)

`src/warehouse.py` loads the curated parquet into a DuckDB database (`data/warehouse/taxi.duckdb`) as a star schema:

```
dim_date ─┐
dim_zone ─┼──  fact_trips  (2,713,337 rows; pickup/dropoff zone, date, hour, payment type, distance, duration, fare, tip, total)
dim_payment_type ─┘        etl_run_log, dq_rejects_by_rule  (the Spark run's data-quality metrics, queryable in SQL)
```

- Dimensions have primary keys and the fact table has **foreign keys**; after loading, 5 integrity checks must pass or the
  build fails: fact rows equal curated rows, summed `total_amount` matches, no NULL keys, no orphan zone/date/payment keys.
- Load time: 14.5 s for 2.7M rows. The five analytical queries (`reports/wh_*.csv`) each run in 6-74 ms:
  revenue by borough and weekday, top 3 routes per pickup borough (window function), 7-day moving average of daily trips
  (window function), airport share of revenue on weekdays vs weekends, and card tip % by borough.
- Example findings: the busiest Manhattan route is Upper East Side South to North (20,998 trips); airports bring in about
  22% of revenue on both weekdays and weekends; Manhattan card trips average a 26.26% tip.
- `python -m src.warehouse` needs the Spark pipeline to have run first; its tests (`tests/test_warehouse.py`) use tiny
  parquet files and need no Spark.

## Orchestration (Prefect)

`src/flow.py` wraps the stages in a [Prefect](https://www.prefect.io/) flow: `download -> spark-etl -> quality-gate -> load-warehouse`.

- The download task retries twice and the warehouse load once, so a transient failure does not fail the run.
- The **quality gate** stops the flow before the warehouse load if more than 15% of rows were rejected (configurable with `--max-reject-rate`).
- Each task and its state is logged by Prefect. `python -m src.flow` ran end to end in about 1.5 minutes (Spark ETL plus warehouse load).
- It runs on demand with a temporary local Prefect server; no scheduled deployment is set up.

## Run it

Requirements: Python 3.10-3.12, Java 11 or 17 (`JAVA_HOME` set).

```bash
pip install -r requirements.txt
python scripts/download_data.py 2024-01      # ~50 MB
python -m src.pipeline --month 2024-01       # Spark ETL: raw -> curated parquet + reports
python -m src.warehouse                      # curated -> star-schema warehouse + analytics
python -m src.flow --month 2024-01           # or run everything through the Prefect flow
python -m pytest -q                          # 16 tests
```

## Tests

`tests/test_pipeline.py` runs against small in-memory DataFrames that use the same column types as the real files. It checks that:
- every rule rejects exactly its own bad row, and that the first failing rule wins;
- NULL values are rejected;
- the quality report's counts add up;
- derived columns are correct;
- the zone join keeps the row count and adds zone names, and tip % is only set for card payments.

## Notes and limits

- Runs in Spark **local mode on one machine** (3M rows). It is a pipeline design project, not a cluster deployment.
- The warehouse is an embedded DuckDB file on the same machine, not a cloud data warehouse such as BigQuery or Redshift.
- Built and tested on **Windows 11** with PySpark 3.5.9 and Java 11. Spark's native parquet writer needs Hadoop's `winutils` on Windows, so on Windows the pipeline writes parquet with PyArrow from the Spark executors instead (`src/spark_session.py`). On Linux/macOS it uses Spark's standard writer; that branch is not tested here.
- Validation thresholds (for example 80 mph, 180 minutes, $500) are sensible defaults chosen for this dataset, not TLC rules.
