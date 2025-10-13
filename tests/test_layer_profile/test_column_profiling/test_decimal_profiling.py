from decimal import Decimal

from respark.layer_profile import (
    DecimalColumnProfile,
    profile_decimal_column,
)


def test_creates_valid_DecimalProfile(sales_df):

    decimal_profile = profile_decimal_column(sales_df, "sale_total_gbp")

    assert isinstance(decimal_profile, DecimalColumnProfile)
    assert decimal_profile.name == "sale_total_gbp"
    assert decimal_profile.normalised_type == "numeric"
    assert decimal_profile.spark_subtype == "decimal"
    assert decimal_profile.nullable is False
    assert decimal_profile.precision == 10
    assert decimal_profile.scale == 2
    assert decimal_profile.min_value == Decimal("129.99")
    assert decimal_profile.max_value == Decimal("987.65")
    assert decimal_profile.mean_value == 515.579
