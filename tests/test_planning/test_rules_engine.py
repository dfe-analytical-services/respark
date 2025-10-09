import pytest
from datetime import date
from pyspark.sql import functions as F, types as T
from respark.planning.generation_rules import (
    get_generation_rule,
    GENERATION_RULES_REGISTRY,
)

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
        min_date="2020-01-01",
        max_date="2020-01-10",
    )
    df = spark.range(5000).select(rule.generate_column().alias("test_date"))

    assert isinstance(df.schema["test_date"].dataType, T.DateType)

    row = df.select(
        F.min("test_date").alias("min_value"), F.max("test_date").alias("max_value")
    ).first()
    assert row.min_value >= date(2020, 1, 1)
    assert row.max_value <= date(2020, 1, 10)


# random_int rule tests
def test_random_int_default_bounds_and_type(spark):
    rule = get_generation_rule("random_int", __row_idx=F.col("id"), __seed=TEST_SEED)
    df = spark.range(1000).select(rule.generate_column().alias("test_int"))

    assert isinstance(df.schema["test_int"].dataType, T.IntegerType)

    row = df.select(
        F.min("test_int").alias("min_value"), F.max("test_int").alias("max_value")
    ).first()
    assert row.min_value >= 0
    assert row.max_value <= 2147483647


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
