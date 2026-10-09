"""SparkSession factory and a writer that also works on Windows without Hadoop's winutils."""
import os
import sys
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
from pyspark.sql import DataFrame, SparkSession


def get_spark(app_name: str = "nyc-taxi-pipeline", shuffle_partitions: int = 16, master: str = "local[*]") -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # workers must use the same interpreter
    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_parquet(spark: SparkSession, path: Path) -> DataFrame:
    """Read a (hive-partitioned) parquet directory.

    On Windows, Hadoop's directory listing needs native helpers that are not installed, so the part
    files are listed explicitly; basePath keeps partition columns such as pickup_date.
    """
    path = Path(path)
    if os.name != "nt":
        return spark.read.parquet(str(path))
    files = sorted(str(p) for p in path.rglob("*.parquet"))
    return spark.read.option("basePath", str(path)).parquet(*files)


def write_parquet(df: DataFrame, path: Path, partition_cols: list[str] | None = None) -> None:
    """Write df as (optionally hive-partitioned) parquet.

    Spark's native writer needs Hadoop's winutils on Windows, so on Windows each Arrow
    batch is written by the executors with PyArrow instead; elsewhere the normal writer is used.
    """
    path = Path(path)
    if os.name != "nt":
        writer = df.write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(str(path))
        return

    path.mkdir(parents=True, exist_ok=True)
    for old in path.rglob("*.parquet"):
        old.unlink()
    out = str(path)

    def write_batches(batches):
        for pdf in batches:
            if len(pdf):
                ds.write_dataset(
                    pa.Table.from_pandas(pdf, preserve_index=False),
                    out,
                    format="parquet",
                    partitioning=partition_cols,
                    partitioning_flavor="hive" if partition_cols else None,
                    basename_template=f"part-{uuid.uuid4().hex}-{{i}}.parquet",
                    existing_data_behavior="overwrite_or_ignore",
                )
            yield pdf.head(0)

    df.mapInPandas(write_batches, schema=df.schema).count()
