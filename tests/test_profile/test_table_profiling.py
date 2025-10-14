import pytest
from respark.profile.column_profiles import (
    StringColumnProfile,
    IntegralColumnProfile,
    DateColumnProfile,
    DecimalColumnProfile,
    FractionalColumnProfile,
    BooleanColumnProfile,
)
from respark.profile import TableProfile, profile_table


def test_profilling_supported_tables(employees_df, departments_df, sales_df):
    employees_table_profile = profile_table(employees_df, "employees")

    assert isinstance(employees_table_profile, TableProfile)
    assert isinstance(
        employees_table_profile.columns["employee_id"], IntegralColumnProfile
    )
    assert isinstance(
        employees_table_profile.columns["first_name"], StringColumnProfile
    )
    assert isinstance(employees_table_profile.columns["last_name"], StringColumnProfile)
    assert isinstance(
        employees_table_profile.columns["department_id"], IntegralColumnProfile
    )
    assert isinstance(
        employees_table_profile.columns["is_current"], BooleanColumnProfile
    )
    assert employees_table_profile.row_count == 10

    departments_table_profile = profile_table(departments_df, "departments")

    assert isinstance(departments_table_profile, TableProfile)
    assert isinstance(
        departments_table_profile.columns["department_id"], IntegralColumnProfile
    )
    assert isinstance(
        departments_table_profile.columns["department_name"], StringColumnProfile
    )

    sales_table_profile = profile_table(sales_df, "sales")

    assert isinstance(sales_table_profile, TableProfile)
    assert isinstance(sales_table_profile.columns["sale_id"], IntegralColumnProfile)
    assert isinstance(sales_table_profile.columns["sale_date"], DateColumnProfile)
    assert isinstance(
        sales_table_profile.columns["sale_total_gbp"], DecimalColumnProfile
    )
    assert isinstance(
        sales_table_profile.columns["delivery_distance_miles"], FractionalColumnProfile
    )
    assert isinstance(
        sales_table_profile.columns["customer_feedback_score"], FractionalColumnProfile
    )


def test_profilling_unsupported_table(invalid_employees_df):
    with pytest.raises(TypeError):
        profile_table(invalid_employees_df, "employees")
