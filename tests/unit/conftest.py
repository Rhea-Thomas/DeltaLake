import pytest


@pytest.fixture(scope="session")
def spark():
    from lakehouse.spark_session import get_spark

    s = get_spark("lakehouse-tests", master="local[2]", shuffle_partitions=1)
    yield s
    s.stop()
