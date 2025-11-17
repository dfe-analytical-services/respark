import pytest
from decimal import Decimal
from pyspark.sql import types as T

from respark.profile.column_profiles.decimal_profile import (
    DecimalColumnProfile,
    profile_decimal_column,
)


@pytest.mark.parametrize(
    "precision, scale, rows, nullable, expected_min, expected_max",
    [
        pytest.param(
            10,
            2,
            [(Decimal("1.23"),), (Decimal("4.56"),), (Decimal("3.21"),)],
            False,
            "1.23",
            "4.56",
            id="p10-s2-simple",
        ),
        pytest.param(
            6,
            3,
            [(Decimal("-2.500"),), (Decimal("1.250"),), (Decimal("3.250"),), (None,)],
            True,
            "-2.500",
            "3.250",
            id="p6-s3-negatives-with-null",
        ),
        pytest.param(
            8,
            2,
            [(None,), (None,)],
            True,
            None,
            None,
            id="p8-s2-all-nulls",
        ),
    ],
)
def test_decimal_profile_scenarios(
    spark, precision, scale, rows, nullable, expected_min, expected_max
):
    schema = T.StructType(
        [
            T.StructField(
                "some_decimal", T.DecimalType(precision, scale), nullable=nullable
            )
        ]
    )
    df = spark.createDataFrame(rows, schema=schema)

    decimal_profile = profile_decimal_column(df, "some_decimal")

    assert isinstance(decimal_profile, DecimalColumnProfile)
    assert decimal_profile.col_name == "some_decimal"
    assert decimal_profile.nullable is nullable
    assert decimal_profile.precision == precision
    assert decimal_profile.scale == scale
    assert decimal_profile.default_rule() == "random_decimal"

    if expected_min is None:
        assert decimal_profile.min_value is None
        assert decimal_profile.max_value is None

    else:
        assert decimal_profile.min_value == expected_min
        assert decimal_profile.max_value == expected_max

    params = decimal_profile.type_specific_params()
    assert params["precision"] == precision
    assert params["scale"] == scale
    if decimal_profile.min_value is None:
        assert params["min_value"] is None
        assert params["max_value"] is None

    else:
        assert params["min_value"] == decimal_profile.min_value
        assert params["max_value"] == decimal_profile.max_value


@pytest.mark.parametrize(
    "dtype, rows",
    [
        pytest.param(T.IntegerType(), [(1,), (2,)], id="int"),
        pytest.param(T.DoubleType(), [(1.0,), (2.5,)], id="double"),
        pytest.param(T.StringType(), [("1.23",), ("4.56",)], id="string"),
        pytest.param(T.BooleanType(), [(True,), (False,)], id="boolean"),
    ],
)
def test_decimal_profile_rejects_non_decimal_types(spark, dtype, rows):
    """
    Negative test: non-DecimalType columns should raise a TypeError.
    """
    schema = T.StructType(
        [T.StructField("some_non_decimal_field", dtype, nullable=True)]
    )
    df = spark.createDataFrame(rows, schema=schema)

    with pytest.raises(TypeError):
        profile_decimal_column(df, "some_non_decimal_field")
