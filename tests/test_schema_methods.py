import pytest
from typing import Dict
from pyspark.sql import types as T
from respark.profiling import (
SchemaProfiler,
SchemaProfile
)


employees_schema = T.StructType([
    T.StructField("first_name", T.StringType(), True),
    T.StructField("last_name", T.StringType(), True),
    T.StructField("department_id", T.IntegerType(), False)
])

employee_data = [
    ("Oliver", "Hughes", 1),
    ("Amelia", "Clark", 2),
    ("Jack", "Turner", 2),
]

department_schema = T.StructType([
    T.StructField("department_id", T.StringType(), True),
    T.StructField("department_name", T.StringType(), True),
])

department_data = [
    (1, "HR"),
    (2, "Software"),
    (3, "Sales")
]

def test_schema_profiling_one_schema(spark):
    employees_df = spark.createDataFrame(employee_data, employees_schema)
    source_data_profiler = SchemaProfiler("employees")
    data_model = source_data_profiler.profile_schema(employees_df)

    assert isinstance(data_model, SchemaProfile)
    assert isinstance(data_model.to_dict(), Dict)
