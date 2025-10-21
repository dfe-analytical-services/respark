import pytest
from pyspark.sql import types as T
from respark.profile.column_profiles import BooleanColumnProfile, profile_boolean_column


@pytest.mark.parametrize(
    "rows, nullable, exp_true, exp_false, exp_null",
    [
        pytest.param(
            [(True,), (False,), (None,)],
            True,
            0.33,
            0.33,
            0.33,
            id="balanced-with-null",
        ),
        pytest.param(
            [(True,), (True,), (False,), (False,)],
            False,
            0.50,
            0.50,
            0.00,
            id="even-no-nulls",
        ),
        pytest.param(
            [(None,), (None,), (None,)],
            True,
            0.00,
            0.00,
            1.00,
            id="all-nulls",
        ),
        pytest.param(
            [(True,), (True,), (True,), (False,), (None,)],
            True,
            0.60,
            0.20,
            0.20,
            id="skewed-with-null",
        ),
    ],
)
def test_boolean_profile_scenarios(
    spark, rows, nullable, exp_true, exp_false, exp_null
):
    schema = T.StructType(
        [T.StructField("some_boolean_field", T.BooleanType(), nullable=nullable)]
    )
    df = spark.createDataFrame(rows, schema=schema)

    boolean_profile = profile_boolean_column(df, "some_boolean_field")

    assert isinstance(boolean_profile, BooleanColumnProfile)
    assert boolean_profile.name == "some_boolean_field"
    assert boolean_profile.normalised_type == "boolean"
    assert boolean_profile.nullable is nullable
    assert boolean_profile.default_rule() == "random_boolean"
    assert boolean_profile.percentage_true == pytest.approx(exp_true, rel=0, abs=0)
    assert boolean_profile.percentage_false == pytest.approx(exp_false, rel=0, abs=0)
    assert boolean_profile.percentage_null == pytest.approx(exp_null, rel=0, abs=0)

    # Params mirror the instance fields
    params = boolean_profile.type_specific_params()
    assert params["percentage_true"] == pytest.approx(
        boolean_profile.percentage_true, rel=0, abs=0
    )
    assert params["percentage_false"] == pytest.approx(
        boolean_profile.percentage_false, rel=0, abs=0
    )
    assert params["percentage_null"] == pytest.approx(
        boolean_profile.percentage_null, rel=0, abs=0
    )
