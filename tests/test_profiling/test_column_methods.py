from respark.profiling import (
    StringColumnProfile,
    NumericColumnProfile,
    DateColumnProfile,
    profile_string_column,
    profile_numerical_column,
    profile_date_column,
)


def test_creates_valid_StringProfile(employees_df):

    first_name_profile = profile_string_column(employees_df, "first_name")

    assert isinstance(first_name_profile, StringColumnProfile)
    assert first_name_profile.name == "first_name"
    assert first_name_profile.normalised_type == "string"
    assert first_name_profile.nullable is False
    assert first_name_profile.min_length == 3
    assert first_name_profile.max_length == 6
    assert first_name_profile.mean_length == 4.3


def test_creates_valid_NumericalProfile(employees_df):

    department_id_profile = profile_numerical_column(employees_df, "department_id")

    assert isinstance(department_id_profile, NumericColumnProfile)
    assert department_id_profile.name == "department_id"
    assert department_id_profile.normalised_type == "numeric"
    assert department_id_profile.nullable is False
    assert department_id_profile.min_value == 1
    assert department_id_profile.max_value == 5
    assert department_id_profile.mean_value == 3


def test_creates_valid_DateProfile(employees_df):

    date_profile = profile_date_column(employees_df, "start_date")

    assert isinstance(date_profile, DateColumnProfile)
    assert date_profile.name == "start_date"
    assert date_profile.normalised_type == "date"
    assert date_profile.nullable is False
    assert date_profile.min_date is not None
    assert date_profile.max_date is not None
    assert date_profile.min_date.isoformat() == "2020-02-01"
    assert date_profile.max_date.isoformat() == "2020-02-10"
