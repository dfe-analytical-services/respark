import pytest
from pyspark.sql import types as T
from respark.profiling import (
StringColumnProfile,
NumericColumnProfile,
TableProfile,
profile_table
)

def test_profilling_supported_table(employees_df):
    employees_table_profile = profile_table(employees_df, "employees")

    assert isinstance(employees_table_profile, TableProfile)
    assert isinstance(employees_table_profile.columns["first_name"], StringColumnProfile)
    assert isinstance(employees_table_profile.columns["last_name"], StringColumnProfile)
    assert isinstance(employees_table_profile.columns["department_id"], NumericColumnProfile)
    assert employees_table_profile.row_count == 10

def test_profilling_unsupported_table(invalid_employees_df):
    with pytest.raises(TypeError):
        employees_table_profile = profile_table(invalid_employees_df, "employees")
        
        