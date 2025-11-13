import string
import pytest
from pyspark.sql import functions as F, types as T
from respark.rules.registry import get_generation_rule


@pytest.mark.parametrize(
    "expected_min, expected_max, ascii_lower, ascii_upper, extra_char",
    [
        pytest.param(2, 5, True, True, "", id="default-settings"),
        pytest.param(2, 5, False, False, "abcde", id="specific-char-only"),
        pytest.param(2, 5, False, False, "äéíóú", id="accented-char-only"),
        pytest.param(2, 5, True, False, "", id="default-lower-char-only"),
        pytest.param(2, 5, False, True, "", id="default-upper-char-only"),
    ],
)
def test_random_string_generation_scenarios(
    spark, test_seed, expected_min, expected_max, ascii_lower, ascii_upper, extra_char
):
    rule = get_generation_rule(
        "random_string",
        __row_idx=F.col("id"),
        __seed=test_seed,
        min_length=expected_min,
        max_length=expected_max,
        ascii_lower=ascii_lower,
        ascii_upper=ascii_upper,
        extra_char=extra_char,
    )
    df = spark.range(100).select(rule.generate_column().alias("test_string"))

    assert isinstance(df.schema["test_string"].dataType, T.StringType)

    length_df = df.select(F.length("test_string").alias("len"))
    row = length_df.select(
        F.min("len").alias("min_value"), F.max("len").alias("max_value")
    ).first()
    assert row.min_value >= expected_min
    assert row.max_value <= expected_max

    legal_char = ""
    if ascii_lower:
        legal_char += string.ascii_lowercase
    if ascii_upper:
        legal_char += string.ascii_uppercase
    if extra_char:
        legal_char += extra_char

    illegal_char = df.where(~F.col("test_string").rlike(f"^[{legal_char}]*$")).count()
    assert illegal_char == 0
