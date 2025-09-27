import pytest
from pyspark.sql import types as T
from respark.profiling import (
StringColumnProfile,
TableProfile,
profile_table
)

supported_schema = T.StructType([
    T.StructField("first_name", T.StringType(), True),
    T.StructField("last_name", T.StringType(), True),
    T.StructField("department_id", T.IntegerType(), False)
])

supported_data = [
    ("Oliver", "Hughes", 1),
    ("Amelia", "Clark", 2),
    ("Jack", "Turner", 2),
]

unsupported_schema = T.StructType([
    T.StructField("first_name", T.StringType(), True),
    T.StructField("last_name", T.StringType(), True),
    T.StructField("is_current", T.BooleanType(), False)
])  

unsupported_data = [
    ("Oliver", "Hughes", False),
    ("Amelia", "Clark", False),
    ("Jack", "Turner", True),
]

def test_profilling_supported_table(spark):
    supported_df = spark.createDataFrame(supported_data, supported_schema)
    employees_table_profile = profile_table(supported_df, "employees")

    assert isinstance(employees_table_profile, TableProfile)
    assert isinstance(employees_table_profile.columns["first_name"], StringColumnProfile)

def test_profilling_unsupported_table(spark):
    with pytest.raises(TypeError):
        unsupported_df = spark.createDataFrame(unsupported_data, unsupported_schema)
        employees_table_profile = profile_table(unsupported_df, "employees")
        
        