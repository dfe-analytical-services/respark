import pytest
from respark.layer_profile import (
    FractionalColumnProfile,
    profile_fractional_column,
)


def test_creates_valid_FractionalProfile(sales_df):

    float_profile = profile_fractional_column(sales_df, "customer_feedback_score")

    assert isinstance(float_profile, FractionalColumnProfile)
    assert float_profile.name == "customer_feedback_score"
    assert float_profile.normalised_type == "numeric"
    assert float_profile.nullable is True
    assert pytest.approx(float_profile.min_value, abs=0.1) == 2.3
    assert pytest.approx(float_profile.max_value, abs=0.1) == 5.0
    assert pytest.approx(float_profile.mean_value, abs=0.1) == 3.66

    double_profile = profile_fractional_column(sales_df, "delivery_distance_miles")

    assert isinstance(double_profile, FractionalColumnProfile)
    assert double_profile.name == "delivery_distance_miles"
    assert double_profile.normalised_type == "numeric"
    assert double_profile.nullable is False
    assert pytest.approx(double_profile.min_value, abs=0.1) == 12.56
    assert pytest.approx(double_profile.max_value, abs=0.1) == 89.01
    assert pytest.approx(double_profile.mean_value, abs=0.1) == 48.73
