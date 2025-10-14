import pytest
from typing import Dict
from respark.profile import SchemaProfile, TableProfile, profile_schema, profile_table


def test_schema_profiling_one_schema(employees_df):

    data_model = profile_schema(employees_df)

    assert isinstance(data_model, SchemaProfile)
    assert isinstance(data_model.to_dict(), Dict)


def test_schema_profiling_multiple_schema(employees_df, departments_df):

    data_model = profile_schema(
        {"employees": employees_df, "departments": departments_df}
    )

    assert isinstance(data_model, SchemaProfile)
    assert isinstance(data_model.to_dict(), Dict)
    assert data_model.tables["employees"].name == "employees"
    assert data_model.tables["departments"].name == "departments"
    assert isinstance(data_model.tables["employees"], TableProfile)


def test_raises_error_when_passed_invalid_df_dict(employees_df, departments_df):

    with pytest.raises(TypeError):
        profile_schema(
            {"employees": "incorrect_string", "departments": False}  # type: ignore
        )
    with pytest.raises(TypeError):
        profile_schema({1: employees_df, 2: departments_df})  # type: ignore
    with pytest.raises(TypeError):
        profile_schema({1: "incorrect_string", 2: False})  # type: ignore
    with pytest.raises(TypeError):
        profile_schema({"incorrect_string"})  # type: ignore
