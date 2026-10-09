"""Aggregate reports computed with Spark SQL on the curated trips."""
from pyspark.sql import DataFrame, SparkSession

QUERIES = {
    "daily_summary": """
        SELECT pickup_date,
               COUNT(*)                       AS trips,
               ROUND(SUM(total_amount), 2)    AS revenue,
               ROUND(AVG(fare_amount), 2)     AS avg_fare,
               ROUND(AVG(trip_distance), 2)   AS avg_distance_miles
        FROM trips
        GROUP BY pickup_date
        ORDER BY pickup_date
    """,
    "hourly_demand_by_borough": """
        SELECT pickup_borough, pickup_hour, COUNT(*) AS trips
        FROM trips
        WHERE pickup_borough IS NOT NULL
        GROUP BY pickup_borough, pickup_hour
        ORDER BY pickup_borough, pickup_hour
    """,
    # window function: the 3 busiest pickup zones inside each borough
    "top_zones_per_borough": """
        SELECT pickup_borough, pickup_zone, trips, avg_fare, zone_rank
        FROM (
            SELECT pickup_borough, pickup_zone,
                   COUNT(*)                   AS trips,
                   ROUND(AVG(fare_amount), 2) AS avg_fare,
                   RANK() OVER (PARTITION BY pickup_borough ORDER BY COUNT(*) DESC) AS zone_rank
            FROM trips
            WHERE pickup_borough IS NOT NULL AND pickup_zone IS NOT NULL
            GROUP BY pickup_borough, pickup_zone
        )
        WHERE zone_rank <= 3
        ORDER BY pickup_borough, zone_rank
    """,
    "payment_and_tips": """
        SELECT payment_type_name,
               COUNT(*)                      AS trips,
               ROUND(AVG(tip_pct), 2)        AS avg_tip_pct,
               ROUND(AVG(total_amount), 2)   AS avg_total
        FROM trips
        GROUP BY payment_type_name
        ORDER BY trips DESC
    """,
}


def run_reports(spark: SparkSession, trips: DataFrame) -> dict[str, DataFrame]:
    trips.createOrReplaceTempView("trips")
    return {name: spark.sql(sql) for name, sql in QUERIES.items()}
