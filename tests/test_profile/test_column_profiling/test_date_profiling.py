import pytest
from datetime import date
from pyspark.sql import types as T


from respark.profile.column_profiles import (
    DateColumnProfile,
    profile_date_column,
)


@pytest.mark.parametrize(
    "rows, nullable, expected_min, expected_max",
    [
        pytest.param(
            [(date(2023, 1, 10),), (None,), (date(2023, 1, 1),), (date(2023, 2, 1),)],
            True,
            date(2023, 1, 1),
            date(2023, 2, 1),
            id="with-nulls-unordered",
        ),
        pytest.param(
            [(date(2020, 5, 5),), (date(2020, 5, 5),)],
            False,
            date(2020, 5, 5),
            date(2020, 5, 5),
            id="single-distinct-date-repeated",
        ),
        pytest.param(
            [(None,), (None,)],
            True,
            None,
            None,
            id="all-nulls",
        ),
    ],
)
def test_date_profile_scenarios(spark, rows, nullable, expected_min, expected_max):
    schema = T.StructType(
        [T.StructField("some_date_field", T.DateType(), nullable=nullable)]
    )
    df = spark.createDataFrame(rows, schema=schema)

    date_profile = profile_date_column(df, "some_date_field")

    assert isinstance(date_profile, DateColumnProfile)
    assert date_profile.name == "some_date_field"
    assert date_profile.normalised_type == "date"
    assert date_profile.nullable is nullable
    assert date_profile.default_rule() == "random_date"

    if expected_min is None:
        assert date_profile.min_date is None
        assert date_profile.max_date is None
        params = date_profile.type_specific_params()
        assert params["min_date"] is None
        assert params["max_date"] is None
    else:
        assert date_profile.min_date == expected_min
        assert date_profile.max_date == expected_max
        params = date_profile.type_specific_params()
        assert params["min_date"] == expected_min.isoformat()
        assert params["max_date"] == expected_max.isoformat()
