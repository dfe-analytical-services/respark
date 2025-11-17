import pytest
from pyspark.sql import types as T
from respark.profile.column_profiles.integral_profile import (
    ByteColumnProfile,
    ShortColumnProfile,
    IntColumnProfile,
    LongColumnProfile,
    profile_integral_column,
)


@pytest.mark.parametrize(
    "dtype, expected_profile_class, expected_default_rule",
    [
        pytest.param(T.ByteType(), ByteColumnProfile, "random_byte", id="byte"),
        pytest.param(T.ShortType(), ShortColumnProfile, "random_short", id="short"),
        pytest.param(T.IntegerType(), IntColumnProfile, "random_int", id="int"),
        pytest.param(T.LongType(), LongColumnProfile, "random_long", id="long"),
    ],
)
def test_profiling_for_each_integral_subtype(
    spark, dtype, expected_profile_class, expected_default_rule
):
    schema = T.StructType([T.StructField("some_integral_field", dtype, nullable=True)])
    df = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,), (None,)], schema=schema)

    profile = profile_integral_column(df, "some_integral_field")

    assert isinstance(profile, expected_profile_class)
    assert profile.col_name == "some_integral_field"
    assert profile.nullable is True
    assert profile.min_value == 1
    assert profile.max_value == 5
    assert profile.mean_value == pytest.approx(3.0, abs=0)
    assert profile.default_rule() == expected_default_rule

    params = profile.type_specific_params()
    assert params["min_value"] == 1
    assert params["max_value"] == 5
    assert params["mean_value"] == pytest.approx(3.0, abs=0)


def test_non_integral_type_raises_error(spark):
    schema = T.StructType(
        [T.StructField("some_string_field", T.StringType(), nullable=True)]
    )
    df = spark.createDataFrame([("apples",), ("bananas",)], schema=schema)

    with pytest.raises(TypeError):
        profile_integral_column(df, "some_string_field")
