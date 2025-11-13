from pyspark.sql import functions as F, types as T
from respark.rules.registry import get_generation_rule
from respark.core import INTEGRAL_BOUNDS


def test_random_int_default_bounds_and_type(spark, test_seed):
    rule = get_generation_rule("random_int", __row_idx=F.col("id"), __seed=test_seed)
    df = spark.range(1000).select(rule.generate_column().alias("test_int"))

    assert isinstance(df.schema["test_int"].dataType, T.IntegerType)

    row = df.select(
        F.min("test_int").alias("min_value"), F.max("test_int").alias("max_value")
    ).first()
    assert row.min_value >= INTEGRAL_BOUNDS["int"]["min_value"]
    assert row.max_value <= INTEGRAL_BOUNDS["int"]["max_value"]


def test_random_int_custom_inclusive_bounds(spark, test_seed):
    rule = get_generation_rule(
        "random_int", __row_idx=F.col("id"), __seed=test_seed, min_value=1, max_value=5
    )
    df = spark.range(2000).select(rule.generate_column().alias("random_int"))

    distinct_vals = {r[0] for r in df.select("random_int").distinct().collect()}
    assert distinct_vals.issubset({1, 2, 3, 4, 5})
