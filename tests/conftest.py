import pytest

from src.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("taxi-pipeline-tests", shuffle_partitions=2, master="local[2]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
