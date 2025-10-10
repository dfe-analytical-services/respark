from respark.layer_profile import (
    DateColumnProfile,
    profile_date_column,
)


def test_creates_valid_DateProfile(sales_df):

    date_profile = profile_date_column(sales_df, "sale_date")

    assert isinstance(date_profile, DateColumnProfile)
    assert date_profile.name == "sale_date"
    assert date_profile.normalised_type == "date"
    assert date_profile.spark_subtype == "date"
    assert date_profile.nullable is False
    assert date_profile.min_date is not None
    assert date_profile.max_date is not None
    assert date_profile.min_date.isoformat() == "2025-01-14"
    assert date_profile.max_date.isoformat() == "2025-10-18"
