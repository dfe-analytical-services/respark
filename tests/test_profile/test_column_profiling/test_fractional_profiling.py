import pytest
from decimal import Decimal
from pyspark.sql import types as T

from respark.profile.column_profiles import (
    FloatColumnProfile,
    DoubleColumnProfile,
    profile_fractional_column,
)


@pytest.mark.parametrize(
    "dtype, rows, nullable, expected_min, expected_max, expected_mean, expected_cls, expected_rule",
    [
        pytest.param(
            T.FloatType(),
            [(1.0,), (2.0,), (3.0,), (None,)],
            True,
            1.0,
            3.0,
            2.0,
            FloatColumnProfile,
            "random_float",
            id="float-with-null",
        ),
        pytest.param(
            T.DoubleType(),
            [(0.1,), (0.2,), (0.4,)],
            False,
            0.1,
            0.4,
            (0.1 + 0.2 + 0.4) / 3.0,
            DoubleColumnProfile,
            "random_double",
            id="double-simple",
        ),
        pytest.param(
            T.FloatType(),
            [(-2.0,), (1.5,), (3.5,), (None,)],
            True,
            -2.0,
            3.5,
            (-2.0 + 1.5 + 3.5) / 3.0,
            FloatColumnProfile,
            "random_float",
            id="float-negatives",
        ),
        pytest.param(
            T.FloatType(),
            [(None,), (None,)],
            True,
            None,
            None,
            None,
            FloatColumnProfile,
            "random_float",
            id="float-all-nulls",
        ),
    ],
)
def test_fractional_profile_edge_cases(
    spark,
    dtype,
    rows,
    nullable,
    expected_min,
    expected_max,
    expected_mean,
    expected_cls,
    expected_rule,
):

    schema = T.StructType(
        [T.StructField("some_fractional_field", dtype, nullable=nullable)]
    )
    df = spark.createDataFrame(rows, schema=schema)

    fractional_profile = profile_fractional_column(df, "some_fractional_field")
    assert isinstance(fractional_profile, expected_cls)
    assert fractional_profile.name == "some_fractional_field"
    assert fractional_profile.normalised_type == "numeric"
    assert fractional_profile.nullable is nullable
    assert fractional_profile.default_rule() == expected_rule

    if expected_min is None:
        assert fractional_profile.min_value is None
        assert fractional_profile.max_value is None
        assert fractional_profile.mean_value is None
    else:
        assert fractional_profile.min_value == pytest.approx(
            expected_min, rel=0, abs=1e-7
        )
        assert fractional_profile.max_value == pytest.approx(
            expected_max, rel=0, abs=1e-7
        )
        assert fractional_profile.mean_value == pytest.approx(
            expected_mean, rel=0, abs=1e-7
        )

    params = fractional_profile.type_specific_params()
    assert params["min_value"] == (
        None
        if fractional_profile.min_value is None
        else pytest.approx(fractional_profile.min_value, abs=1e-7)
    )
    assert params["max_value"] == (
        None
        if fractional_profile.max_value is None
        else pytest.approx(fractional_profile.max_value, abs=1e-7)
    )
    if fractional_profile.mean_value is None:
        assert params["mean_value"] is None
    else:
        assert params["mean_value"] == pytest.approx(
            fractional_profile.mean_value, abs=1e-7
        )


@pytest.mark.parametrize(
    "build_df",
    [
        pytest.param(
            lambda spark: spark.createDataFrame(
                [(1,), (2,)],
                schema=T.StructType(
                    [T.StructField("some_non_fractional_field", T.IntegerType(), True)]
                ),
            ),
            id="int",
        ),
        pytest.param(
            lambda spark: spark.createDataFrame(
                [("a",), ("b",)],
                schema=T.StructType(
                    [T.StructField("some_non_fractional_field", T.StringType(), True)]
                ),
            ),
            id="string",
        ),
        pytest.param(
            lambda spark: spark.createDataFrame(
                [(True,), (False,)],
                schema=T.StructType(
                    [T.StructField("some_non_fractional_field", T.BooleanType(), True)]
                ),
            ),
            id="boolean",
        ),
        pytest.param(
            lambda spark: spark.createDataFrame(
                [(Decimal("1.23"),), (Decimal("4.56"),)],
                schema=T.StructType(
                    [
                        T.StructField(
                            "some_non_fractional_field", T.DecimalType(10, 2), True
                        )
                    ]
                ),
            ),
            id="decimal",
        ),
    ],
)
def test_fractional_profile_rejects_non_fractional_types(spark, build_df):
    df = build_df(spark)
    with pytest.raises(TypeError):
        profile_fractional_column(df, "some_non_fractional_field")
