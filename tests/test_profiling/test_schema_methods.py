import pytest
from typing import Dict
from pyspark.sql import types as T
from respark.profiling import (
SchemaProfiler,
SchemaProfile
)
from respark.profiling.profiler_table import TableProfile


def test_schema_profiling_one_schema(employees_df):

    source_data_profiler = SchemaProfiler("employees")
    data_model = source_data_profiler.profile_schema(employees_df)

    assert isinstance(data_model, SchemaProfile)
    assert isinstance(data_model.to_dict(), Dict)

def test_schema_profiling_multiple_schema(employees_df, departments_df):
    source_data_profiler = SchemaProfiler()
    
    data_model = source_data_profiler.profile_schema( {
            "employees":employees_df,
            "departments": departments_df
            })

    assert isinstance(data_model, SchemaProfile)
    assert isinstance(data_model.to_dict(), Dict)
    assert data_model.tables["employees"].name == "employees"
    assert data_model.tables["departments"].name == "departments"
    assert isinstance(data_model.tables["employees"], TableProfile)


def test_raises_error_when_passed_invalid_df_dict(employees_df, departments_df):

    source_data_profiler = SchemaProfiler()
    with pytest.raises(TypeError):
        data_model = source_data_profiler.profile_schema(
            {                                   # type: ignore
            "employees":"incorrect_string",
            "departments": False
            })
    with pytest.raises(TypeError):
        data_model = source_data_profiler.profile_schema(
            {                                   # type: ignore
            1:employees_df,
            2:departments_df
            })
    with pytest.raises(TypeError):
        data_model = source_data_profiler.profile_schema(
            {                                   # type: ignore
            1:"incorrect_string",
            2:False
            })
    with pytest.raises(TypeError):
        data_model = source_data_profiler.profile_schema(
            {"incorrect_string"}               # type: ignore
            )
    
    