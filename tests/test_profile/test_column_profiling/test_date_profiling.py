import pytest
from datetime import date, datetime, UTC
from pyspark.sql import types as T

from respark.profile.column_profiles import (
    DateColumnProfile,
    TimestampColumnProfile,
    TimestampNTZColumnProfile,
    profile_datetime_column,
)


@pytest.mark.parametrize(
    "rows, nullable, expected_min, expected_max",
    [
        pytest.param(
            [(date(2023, 1, 10),), (None,), (date(2023, 1, 1),), (date(2023, 2, 1),)],
            True,
            "2023-01-01",
            "2023-02-01",
            id="with-nulls-unordered",
        ),
        pytest.param(
            [(date(2020, 5, 5),), (date(2020, 5, 5),)],
            False,
            "2020-05-05",
            "2020-05-05",
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

    date_profile = profile_datetime_column(df, "some_date_field")

    assert isinstance(date_profile, DateColumnProfile)
    assert date_profile.name == "some_date_field"
    assert date_profile.normalised_type == "datetime"
    assert date_profile.nullable is nullable
    assert date_profile.default_rule() == "random_date"

    if expected_min is None:
        assert date_profile.min_iso is None
        assert date_profile.max_iso is None
        params = date_profile.type_specific_params()
        assert params["min_iso"] is None
        assert params["max_iso"] is None
    else:
        assert date_profile.min_iso == expected_min
        assert date_profile.max_iso == expected_max
        params = date_profile.type_specific_params()
        assert params["min_iso"] == expected_min
        assert params["max_iso"] == expected_max


def test_timestamp_ltz_profile_basic(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.timestampType", "TIMESTAMP_LTZ")

    rows = [
        (datetime(2024, 5, 1, 12, 30, 0, tzinfo=UTC),),
        (datetime(2024, 5, 1, 12, 30, 00, 123456, tzinfo=UTC),),
        (None,),
    ]

    schema = T.StructType(
        [T.StructField("some_timestamp_field", T.TimestampType(), nullable=True)]
    )
    df = spark.createDataFrame(rows, schema=schema)

    timestamp_profile = profile_datetime_column(df, "some_timestamp_field")

    assert isinstance(timestamp_profile, TimestampColumnProfile)
    assert timestamp_profile.name == "some_timestamp_field"
    assert timestamp_profile.normalised_type == "datetime"
    assert timestamp_profile.default_rule() == "random_timestamp_ltz"

    assert (
        timestamp_profile.min_iso is not None and timestamp_profile.max_iso is not None
    )
    assert timestamp_profile.min_iso.startswith("2024-05-01T12:30:00")
    assert timestamp_profile.max_iso.startswith("2024-05-01T12:30:00")
    assert timestamp_profile.min_iso <= timestamp_profile.max_iso

    assert timestamp_profile.session_time_zone == "UTC"
    assert timestamp_profile.spark_timestamp_alias in ("TIMESTAMP_LTZ", "TIMESTAMP_NTZ")
    assert (timestamp_profile.frac_precision is None) or (
        0 <= timestamp_profile.frac_precision <= 6
    )


def test_timestamp_ltz_all_nulls(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    schema = T.StructType(
        [T.StructField("some_timestamp_field", T.TimestampType(), nullable=True)]
    )
    df = spark.createDataFrame([(None,), (None,)], schema=schema)

    timestamp_profile = profile_datetime_column(df, "some_timestamp_field")
    assert timestamp_profile.normalised_type == "datetime"
    assert timestamp_profile.min_iso is None and timestamp_profile.max_iso is None


def test_timestamp_ntz_profile_basic(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")

    rows = [
        (datetime(2024, 1, 1, 0, 0, 0),),
        (datetime(2024, 1, 1, 23, 59, 59, 500000),),
        (None,),
    ]

    schema = T.StructType(
        [T.StructField("some_timestamp_field", T.TimestampNTZType(), nullable=True)]
    )
    df = spark.createDataFrame(rows, schema=schema)

    timestamp_profile = profile_datetime_column(df, "some_timestamp_field")

    assert isinstance(timestamp_profile, TimestampNTZColumnProfile)
    assert timestamp_profile.normalised_type == "datetime"
    assert timestamp_profile.default_rule() == "random_timestamp_ntz"

    assert (
        timestamp_profile.min_iso is not None and timestamp_profile.max_iso is not None
    )
    assert timestamp_profile.min_iso.startswith("2024-01-01T00:00:00")
    assert timestamp_profile.max_iso.startswith("2024-01-01T23:59:59")
    assert timestamp_profile.min_iso <= timestamp_profile.max_iso

    assert (timestamp_profile.frac_precision is None) or (
        0 <= timestamp_profile.frac_precision <= 6
    )


def test_timestamp_ntz_all_nulls(spark):
    schema = T.StructType(
        [T.StructField("some_timestamp_field", T.TimestampNTZType(), nullable=True)]
    )
    df = spark.createDataFrame([(None,), (None,)], schema=schema)

    timestamp_profile = profile_datetime_column(df, "some_timestamp_field")
    assert timestamp_profile.normalised_type == "datetime"
    assert timestamp_profile.min_iso is None and timestamp_profile.max_iso is None
