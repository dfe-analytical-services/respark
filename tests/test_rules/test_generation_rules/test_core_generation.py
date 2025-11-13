import pytest
from pyspark.sql import functions as F
from respark.rules.registry import get_generation_rule


def test_get_generation_rule_unknown_raises():
    with pytest.raises(ValueError):
        get_generation_rule("some_rule_that_does_not_exist")

###
#Test Random from Set
###

def test_random_from_set_selects_from_valid_options(spark, test_seed):
    rule = get_generation_rule(
        "random_from_set",
        __row_idx=F.col("id"),
        __seed=test_seed,
        valid_options=["a", "b", "c"]
    )

    df = spark.range(5000).select(rule.generate_column().alias("test_random_from_set"))

    assert set(r["test_random_from_set"] for r in df.distinct().collect()) == {"a", "b", "c"}

def test_random_from_set_raises_with_empty_valid_options(test_seed):
    rule = get_generation_rule(
        "random_from_set",
        __row_idx=F.col("id"),
        __seed=test_seed,
        valid_options=[]
    )

    with pytest.raises(ValueError):
        rule.generate_column().alias("test_random_from_set")



