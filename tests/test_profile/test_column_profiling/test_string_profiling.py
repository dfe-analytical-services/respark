from respark.profile.column_profiles import (
    StringColumnProfile,
    profile_string_column,
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
