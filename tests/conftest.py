import pytest
from typing import cast
from pyspark.sql import SparkSession, DataFrame
from respark.profiling import SchemaProfiler, SchemaProfile
from respark.planning import SchemaGenerationPlan, make_generation_plan
from data.mock_production_tables import (
    employees_schema,
    employees_rows,
    departments_schema,
    departments_rows,
    invalid_employees_schema,
    invalid_employees_rows,
)


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
def employees_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(employees_rows, schema=employees_schema)


@pytest.fixture(scope="session")
def departments_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(departments_rows, schema=departments_schema)


@pytest.fixture(scope="session")
def invalid_employees_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        invalid_employees_rows, schema=invalid_employees_schema
    )


@pytest.fixture(scope="session")
def mock_schema_profile(employees_df, departments_df) -> SchemaProfile:
    mock_profiler = SchemaProfiler()
    return mock_profiler.profile_schema(
        {
            "employees": employees_df,
            "departments": departments_df,
        }
    )


@pytest.fixture(scope="session")
def mock_schema_gen_plan(mock_schema_profile) -> SchemaGenerationPlan:
    return make_generation_plan(mock_schema_profile)
