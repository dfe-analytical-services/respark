import pytest
from typing import cast
from pyspark.sql import SparkSession, DataFrame

from .data import (
    employees_schema,
    employees_rows,
    departments_schema,
    departments_rows,
    sales_schema,
    sales_rows,
    invalid_employees_schema,
    invalid_employees_rows,
)

TEST_SEED = 2025


# Making a spark session available to pytest during testing
@pytest.fixture(scope="session")
def spark():
    builder = cast(SparkSession.Builder, SparkSession.builder)
    spark = (
        builder.master("local[*]")
        .appName("respark-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        yield spark
    finally:
        spark.stop()


@pytest.fixture(scope="session")
def test_seed() -> int:
    return TEST_SEED


###
# Create Mock DataFrames, both valid and invalid
###
@pytest.fixture(scope="session")
def employees_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(employees_rows, schema=employees_schema)


@pytest.fixture(scope="session")
def departments_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(departments_rows, schema=departments_schema)


@pytest.fixture(scope="session")
def sales_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(sales_rows, schema=sales_schema)


@pytest.fixture(scope="session")
def invalid_employees_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        invalid_employees_rows, schema=invalid_employees_schema
    )
