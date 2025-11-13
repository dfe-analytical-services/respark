import pytest
from datetime import date
from pyspark.sql import functions as F, types as T
from respark.rules.registry import get_generation_rule, GENERATION_RULES_REGISTRY
from respark.core import INTEGRAL_BOUNDS

TEST_SEED = 2025


def test_known_rules_are_registered():

    assert "random_date" in GENERATION_RULES_REGISTRY
    assert "random_int" in GENERATION_RULES_REGISTRY
    assert "random_string" in GENERATION_RULES_REGISTRY


def test_get_generation_rule_unknown_raises():
    with pytest.raises(ValueError):
        get_generation_rule("nope")


# random_date rule tests
def test_random_date_bounds_and_type(spark):
    rule = get_generation_rule(
        "random_date",
        __row_idx=F.col("id"),
        __seed=TEST_SEED,
        min_iso="2020-01-01",
        max_iso="2020-01-10",
    )
    df = spark.range(5000).select(rule.generate_column().alias("test_date"))

    assert isinstance(df.schema["test_date"].dataType, T.DateType)

    row = df.select(
        F.min("test_date").alias("min_value"), F.max("test_date").alias("max_value")
    ).first()
    assert row.min_value >= date(2020, 1, 1)
    assert row.max_value <= date(2020, 1, 10)


def test_random_timestamp_ltz_bounds_and_type(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    min_iso = "2020-01-01T00:00:00.000000"
    max_iso = "2025-12-31T23:59:59.999999"
    min_epoch_micros = "1577836800000000"
    max_epoch_micros = "1767225599999999"

    rule = get_generation_rule(
        "random_timestamp_ltz",
        __row_idx=F.col("id"),
        __seed=TEST_SEED,
        min_iso=min_iso,
        max_iso=max_iso,
        min_epoch_micros=min_epoch_micros,
        max_epoch_micros=max_epoch_micros,
    )

    df = spark.range(5000).select(rule.generate_column().alias("test_timestamp_ltz"))

    assert isinstance(df.schema["test_timestamp_ltz"].dataType, T.TimestampType)

    row = df.select(
        F.min(F.unix_micros("test_timestamp_ltz")).alias("min_value"),
        F.max(F.unix_micros("test_timestamp_ltz")).alias("max_value"),
    ).first()

    assert row.min_value >= int(min_epoch_micros)
    assert row.max_value <= int(max_epoch_micros)


def test_random_timestamp_ntz_bounds_and_type(spark):
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    min_iso = "2020-01-01T00:00:00.000000"
    max_iso = "2025-12-31T23:59:59.999999"

    rule = get_generation_rule(
        "random_timestamp_ntz",
        __row_idx=F.col("id"),
        __seed=TEST_SEED,
        min_iso=min_iso,
        max_iso=max_iso,
    )

    df = spark.range(5000).select(rule.generate_column().alias("test_timestamp_ntz"))

    assert isinstance(df.schema["test_timestamp_ntz"].dataType, T.TimestampNTZType)

    row = df.select(
        F.to_date(F.min("test_timestamp_ntz")).alias("min_value"),
        F.to_date(F.max("test_timestamp_ntz")).alias("max_value"),
    ).first()

    assert row.min_value >= date(2020, 1, 1)
    assert row.max_value <= date(2025, 12, 31)


# random_int rule tests
def test_random_int_default_bounds_and_type(spark):
    rule = get_generation_rule("random_int", __row_idx=F.col("id"), __seed=TEST_SEED)
    df = spark.range(1000).select(rule.generate_column().alias("test_int"))

    assert isinstance(df.schema["test_int"].dataType, T.IntegerType)

    row = df.select(
        F.min("test_int").alias("min_value"), F.max("test_int").alias("max_value")
    ).first()
    assert row.min_value >= INTEGRAL_BOUNDS["int"]["min_value"]
    assert row.max_value <= INTEGRAL_BOUNDS["int"]["max_value"]


def test_random_int_custom_inclusive_bounds(spark):
    rule = get_generation_rule(
        "random_int", __row_idx=F.col("id"), __seed=TEST_SEED, min_value=1, max_value=5
    )
    df = spark.range(2000).select(rule.generate_column().alias("random_int"))

    distinct_vals = {r[0] for r in df.select("random_int").distinct().collect()}
    assert distinct_vals.issubset({1, 2, 3, 4, 5})


# random_string rule tests
def test_random_string_length_and_charset(spark):
    rule = get_generation_rule(
        "random_string",
        __row_idx=F.col("id"),
        __seed=TEST_SEED,
        min_length=2,
        max_length=5,
        charset="abcde",
    )
    df = spark.range(2000).select(rule.generate_column().alias("test_string"))

    assert isinstance(df.schema["test_string"].dataType, T.StringType)

    length_df = df.select(F.length("test_string").alias("len"))
    row = length_df.select(
        F.min("len").alias("min_value"), F.max("len").alias("max_value")
    ).first()
    assert row.min_value >= 2
    assert row.max_value <= 5

    illegal_char = df.where(~F.col("test_string").rlike(r"^[abcde]*$")).count()
    assert illegal_char == 0
