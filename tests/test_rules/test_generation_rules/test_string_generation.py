from pyspark.sql import functions as F, types as T
from respark.rules.registry import get_generation_rule


def test_random_string_length_and_charset(spark, test_seed):
    rule = get_generation_rule(
        "random_string",
        __row_idx=F.col("id"),
        __seed=test_seed,
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
