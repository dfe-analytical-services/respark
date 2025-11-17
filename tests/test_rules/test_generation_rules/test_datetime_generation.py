from datetime import date
from pyspark.sql import functions as F, types as T
from respark.rules.registry import get_generation_rule

###
# Testing Date Generation
###


def test_random_date_bounds_and_type(spark, test_seed):
    rule = get_generation_rule(
        "random_date",
        __row_idx=F.col("id"),
        __seed=test_seed,
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


###
# Testing Datetime LTZ Generation
###


def test_random_timestamp_ltz_bounds_and_type(spark, test_seed):
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    min_epoch_micros = "1577836800000000"
    max_epoch_micros = "1767225599999999"

    rule = get_generation_rule(
        "random_timestamp_ltz",
        __row_idx=F.col("id"),
        __seed=test_seed,
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


###
# Testing Datetime NTZ Generation
###


def test_random_timestamp_ntz_bounds_and_type(spark, test_seed):
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    min_epoch_micros = "1577836800000000"
    max_epoch_micros = "1767225599999999"

    rule = get_generation_rule(
        "random_timestamp_ntz",
        __row_idx=F.col("id"),
        __seed=test_seed,
        min_epoch_micros=min_epoch_micros,
        max_epoch_micros=max_epoch_micros,
    )

    df = spark.range(5000).select(rule.generate_column().alias("test_timestamp_ntz"))

    assert isinstance(df.schema["test_timestamp_ntz"].dataType, T.TimestampNTZType)

    row = df.select(
        F.to_date(F.min("test_timestamp_ntz")).alias("min_value"),
        F.to_date(F.max("test_timestamp_ntz")).alias("max_value"),
    ).first()

    assert row.min_value >= date(2020, 1, 1)
    assert row.max_value <= date(2025, 12, 31)
