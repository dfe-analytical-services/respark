from pyspark.sql import functions as F, types as T
from respark.rules.registry import get_generation_rule


###
# Testing Boolean Generation
###


def test_random_bool_ensure_correct_type(spark, test_seed):
    rule = get_generation_rule(
        "random_boolean",
        __row_idx=F.col("id"),
        __seed=test_seed,
        percentage_true=0.5,
    )
    df = spark.range(5000).select(rule.generate_column().alias("test_bool"))

    assert isinstance(df.schema["test_bool"].dataType, T.BooleanType)


def test_random_bool_follows_distribution(spark, test_seed):
    most_true_rule = get_generation_rule(
        "random_boolean",
        __row_idx=F.col("id"),
        __seed=test_seed,
        percentage_true=0.9,
    )
    most_true_df = spark.range(5000).select(
        most_true_rule.generate_column().alias("test_bool")
    )
    most_true_row = most_true_df.select(
        F.count_if(F.col("test_bool") == True).alias("true_count")
    ).first()
    assert most_true_row.true_count > 4000

    all_false_rule = get_generation_rule(
        "random_boolean",
        __row_idx=F.col("id"),
        __seed=test_seed,
        percentage_true=0.0,
    )
    all_false_df = spark.range(5000).select(
        all_false_rule.generate_column().alias("test_bool")
    )
    all_false_row = all_false_df.select(
        F.count_if(F.col("test_bool") == True).alias("true_count")
    ).first()
    assert all_false_row.true_count == 0
