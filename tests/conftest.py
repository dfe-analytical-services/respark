import pytest
from typing import Dict, cast
from pyspark.sql import SparkSession, DataFrame
from respark.layer_profile import SchemaProfiler, SchemaProfile
from respark.planning import SchemaGenerationPlan, make_generation_plan
from respark.executing import SynthSchemaGenerator
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


# Valid Dataframes
@pytest.fixture(scope="session")
def employees_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(employees_rows, schema=employees_schema)


@pytest.fixture(scope="session")
def departments_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(departments_rows, schema=departments_schema)


@pytest.fixture(scope="session")
def sales_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(sales_rows, schema=sales_schema)


# Invalid Dataframes
@pytest.fixture(scope="session")
def invalid_employees_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        invalid_employees_rows, schema=invalid_employees_schema
    )


# Mock Schemas
@pytest.fixture(scope="session")
def mock_schema_profile(employees_df, departments_df, sales_df) -> SchemaProfile:
    mock_profiler = SchemaProfiler()
    return mock_profiler.profile_schema(
        {"employees": employees_df, "departments": departments_df, "sales": sales_df}
    )


@pytest.fixture(scope="session")
def mock_schema_gen_plan(mock_schema_profile) -> SchemaGenerationPlan:
    return make_generation_plan(mock_schema_profile)


@pytest.fixture(scope="session")
def mock_synth_schema(spark, mock_schema_gen_plan) -> Dict[str, DataFrame]:
    mock_schema_generator = SynthSchemaGenerator(spark)
    return mock_schema_generator.generate_synthetic_schema(spark, mock_schema_gen_plan)
