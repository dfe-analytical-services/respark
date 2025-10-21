import pytest
from typing import List
from pyspark.sql import functions as F, types as T
from respark.random import RNG
from respark.random.random_sampling import (
    randint_long,
    randint_int,
    randint_short,
    randint_byte,
    choice,
)


def make_id_df(spark, n: int):
    """DF with a stable per-row identity suitable for RNG: row_id + row_idx."""
    df = spark.range(0, n).withColumnRenamed("id", "row_id")
    return df.withColumn("row_idx", F.xxhash64(F.col("row_id")))


###
# Testing random_int functions
###


def test_randint_long_invalid_span_raises(spark, test_seed):
    df = make_id_df(spark, 100)
    rng = RNG(row_idx=df["row_idx"], base_seed=test_seed)
    with pytest.raises(ValueError):
        df.select(randint_long(rng, 10, 1, "salt").alias("x")).collect()


@pytest.mark.parametrize(
    "fn,min_value,max_value,expected_type",
    [
        (randint_long, -50000, 50000, T.LongType),
        (randint_int, -10, 10, T.IntegerType),
        (randint_short, -100, 100, T.ShortType),
        (randint_byte, -128, 127, T.ByteType),
    ],
)
def test_randint_bounds_and_types(
    spark, test_seed, fn, min_value, max_value, expected_type
):
    df = make_id_df(spark, 5000)
    rng = RNG(row_idx=df["row_idx"], base_seed=test_seed)

    out = df.select(fn(rng, min_value, max_value, "bounds").alias("x"))

    assert isinstance(out.schema["x"].dataType, expected_type)
    generated_min_value, generated_max_value = out.agg(F.min("x"), F.max("x")).first()
    assert generated_min_value >= min_value and generated_max_value <= max_value


###
# Testing random choice function
###


def test_choice_from_python_list_basic(spark, test_seed):
    df = make_id_df(spark, 5000)
    rng = RNG(row_idx=df["row_idx"], base_seed=test_seed)

    fruits: List[str] = ["apple", "banana", "cranberry", "date"]
    out = df.select(choice(rng, fruits, "colors").alias("color"))

    distinct = set(r["color"] for r in out.distinct().collect())
    assert distinct.issubset(set(fruits))


def test_choice_from_python_list_empty_returns_nulls(spark, test_seed):
    df = make_id_df(spark, 100)
    rng = RNG(row_idx=df["row_idx"], base_seed=test_seed)

    out = df.select(choice(rng, [], "empty").alias("c"))
    assert out.filter(F.col("c").isNull()).count() == df.count()
