import pytest

from respark.profile.column_profiles import BooleanColumnProfile, profile_boolean_column


def test_creates_valid_DateProfile(employees_df):

    boolean_profile = profile_boolean_column(employees_df, "is_current")

    assert isinstance(boolean_profile, BooleanColumnProfile)
    assert boolean_profile.name == "is_current"
    assert boolean_profile.normalised_type == "boolean"
    assert boolean_profile.spark_subtype == "boolean"
    assert boolean_profile.nullable is False
    assert pytest.approx(boolean_profile.percentage_true, abs=0.05) == 0.7
    assert pytest.approx(boolean_profile.percentage_false, abs=0.05) == 0.3
    assert pytest.approx(boolean_profile.percentage_null, abs=0.05) == 0.0
