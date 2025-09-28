import pytest
from typing import cast
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    builder = cast(SparkSession.Builder, SparkSession.builder)
    spark = (
        builder
        .master("local[*]")
        .appName("respark-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        yield spark
    finally:
        spark.stop()
