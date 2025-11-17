from typing import Dict
from respark.profile import SchemaProfile, TableProfile, profile_schema, profile_table


def test_schema_profiling_one_schema(employees_df):

    data_model = profile_schema({"employees": employees_df})

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
