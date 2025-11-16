import pytest
from pyspark.sql import types as T
from respark.profile.column_profiles import BooleanColumnProfile, profile_boolean_column


@pytest.mark.parametrize(
    "rows, nullable, exp_p_true",
    [
        pytest.param(
            [(True,), (False,), (None,)],
            True,
            0.5,
            id="balanced-with-null",
        ),
        pytest.param(
            [(True,), (True,), (False,), (False,)],
            False,
            0.5,
            id="even-no-nulls",
        ),
        pytest.param(
            [(None,), (None,), (None,)],
            True,
            None,
            id="all-nulls",
        ),
        pytest.param(
            [(True,), (True,), (True,), (False,), (None,)],
            True,
            0.75,
            id="skewed-with-null",
        ),
    ],
)
def test_boolean_profile_scenarios(
    spark,
    rows,
    nullable,
    exp_p_true,
):
    schema = T.StructType(
        [T.StructField("some_boolean_field", T.BooleanType(), nullable=nullable)]
    )
    df = spark.createDataFrame(rows, schema=schema)

    boolean_profile = profile_boolean_column(df, "some_boolean_field")

    assert isinstance(boolean_profile, BooleanColumnProfile)
    assert boolean_profile.col_name == "some_boolean_field"
    assert boolean_profile.nullable is nullable
    assert boolean_profile.default_rule() == "random_boolean"
    assert boolean_profile.p_true == pytest.approx(exp_p_true)

    params = boolean_profile.type_specific_params()
    assert params["p_true"] == pytest.approx(boolean_profile.p_true)
