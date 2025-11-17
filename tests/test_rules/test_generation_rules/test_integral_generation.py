import pytest
from pyspark.sql import functions as F, types as T
from respark.rules.registry import get_generation_rule
from respark.core import INTEGRAL_BOUNDS, INTEGRAL_TYPE


@pytest.mark.parametrize(
    "integral_type",
    [
        pytest.param("byte"),
        pytest.param("short"),
        pytest.param("int"),
        pytest.param("long"),
    ],
)
def test_integral_generation_scenarios(
    spark,
    test_seed,
    integral_type,
):
    rule = get_generation_rule(
        f"random_{integral_type}",
        __row_idx=F.col("id"),
        __seed=test_seed,
        min_value=INTEGRAL_BOUNDS[integral_type]["min_value"],
        max_value=INTEGRAL_BOUNDS[integral_type]["max_value"],
    )

    df = spark.range(5000).select(rule.generate_column().alias("test_integrals"))

    assert isinstance(
        df.schema["test_integrals"].dataType, INTEGRAL_TYPE[integral_type]
    )

    row = df.select(
        F.min("test_integrals").alias("min_value"),
        F.max("test_integrals").alias("max_value"),
    ).first()
    assert row.min_value >= INTEGRAL_BOUNDS[integral_type]["min_value"]
    assert row.max_value <= INTEGRAL_BOUNDS[integral_type]["max_value"]


def test_random_int_custom_inclusive_bounds(spark, test_seed):
    rule = get_generation_rule(
        "random_int", __row_idx=F.col("id"), __seed=test_seed, min_value=1, max_value=5
    )
    df = spark.range(2000).select(rule.generate_column().alias("random_int"))

    distinct_vals = {r[0] for r in df.select("random_int").distinct().collect()}
    assert distinct_vals.issubset({1, 2, 3, 4, 5})
