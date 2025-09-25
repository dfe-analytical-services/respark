import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("respark-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        yield spark
    finally:
        spark.stop()
