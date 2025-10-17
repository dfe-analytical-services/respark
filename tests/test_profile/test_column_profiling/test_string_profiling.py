import pytest
from pyspark.sql import types as T

from respark.profile.column_profiles import (
    StringColumnProfile,
    profile_string_column,
)


@pytest.mark.parametrize(
    "rows, nullable, expected_min, expected_max, expected_mean",
    [
        pytest.param(
            [("apple",), (None,), ("cranberry",)],
            True,
            5,
            9,
            7.0,
            id="with-nulls",
        ),
        pytest.param(
            [("",), ("banana",), ("cranberry",)],
            False,
            0,
            9,
            5.0,
            id="includes-empty-string",
        ),
        pytest.param(
            [(None,), (None,)],
            True,
            None,
            None,
            None,
            id="all-nulls",
        ),
    ],
)
def test_string_profile_scenarios(
    spark, rows, nullable, expected_min, expected_max, expected_mean
):
    schema = T.StructType(
        [T.StructField("some_string", T.StringType(), nullable=nullable)]
    )
    df = spark.createDataFrame(rows, schema=schema)

    string_profile = profile_string_column(df, "some_string")

    assert isinstance(string_profile, StringColumnProfile)
    assert string_profile.name == "some_string"
    assert string_profile.normalised_type == "string"
    assert string_profile.nullable is nullable

    assert string_profile.default_rule() == "random_string"

    if expected_min is None:
        assert string_profile.min_length is None
        assert string_profile.max_length is None
        assert string_profile.mean_length is None
    else:
        assert string_profile.min_length == expected_min
        assert string_profile.max_length == expected_max
        assert string_profile.mean_length == pytest.approx(expected_mean)

    params = string_profile.type_specific_params()
    assert params["min_length"] == string_profile.min_length
    assert params["max_length"] == string_profile.max_length
    if string_profile.mean_length is None:
        assert params["mean_length"] is None
    else:
        assert params["mean_length"] == pytest.approx(string_profile.mean_length)
