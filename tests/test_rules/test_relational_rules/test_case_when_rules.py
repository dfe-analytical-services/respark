import pytest
from typing import Any, cast

from pyspark.sql import functions as F, types as T
from respark.rules.relational_rules.case_when import (
    CaseWhenRule,
    WhenThenConditional,
    ThenAction,
    DefaultCase,
)


class MockRuntime:
    """
    A simple mock runtime with the attributes used by the rules.
    """

    def __init__(self): ...


def create_test_parent_col(spark, rows, schema):
    return spark.createDataFrame(rows, schema).withColumn(
        "__row_idx", F.monotonically_increasing_id()
    )


def test_expr_branches_first_match_wins(spark):
    rows = [(95,), (80,), (55,), (40,)]

    parent_col_df = create_test_parent_col(
        spark, rows, T.StructType([T.StructField("score", T.IntegerType())])
    )

    rule = CaseWhenRule(
        branches=[
            WhenThenConditional("`score` >= 90", ThenAction(then_expr="'A'")),
            WhenThenConditional("`score` >= 75", ThenAction(then_expr="'B'")),
            WhenThenConditional("`score` >= 50", ThenAction(then_expr="'C'")),
        ],
        default_case=DefaultCase(then=ThenAction(then_expr="'D'")),
        __seed=20250311,
        __row_idx=F.col("__row_idx"),
        __table="t",
        __column="priority",
    )

    runtime = MockRuntime()

    output = rule.apply(
        runtime=cast(Any, runtime), base_df=parent_col_df, target_col="grade"
    )
    vals = [r["grade"] for r in output.select("grade").orderBy("score").collect()]
    assert vals == ["D", "C", "B", "A"]


def test_then_rule_branch_invokes_subrule_and_params_flow(spark):
    rows = [(True,), (False,), (True,)]
    parent_col_df = create_test_parent_col(
        spark,
        rows,
        T.StructType([T.StructField("is_current_employee", T.BooleanType())]),
    )

    rule = CaseWhenRule(
        branches=[
            WhenThenConditional(
                "`is_current_employee` = TRUE",
                ThenAction(
                    then_rule="const_literal", then_params={"value": "some_string"}
                ),
            ),
        ],
        default_case=DefaultCase(then=ThenAction(then_expr="NULL")),
        __seed=20250311,
        __row_idx=F.col("__row_idx"),
        __table="t",
        __column="child_col",
    )

    runtime = MockRuntime()
    output = rule.apply(
        runtime=cast(Any, runtime), base_df=parent_col_df, target_col="child_col"
    )

    assert [r["child_col"] for r in output.select("child_col").collect()] == [
        "some_string",
        None,
        "some_string",
    ]


def test_then_expr_can_reference_other_columns(spark):

    rows = [(True, 10), (False, 20)]
    parent_col_df = create_test_parent_col(
        spark,
        rows,
        T.StructType(
            [
                T.StructField("A", T.BooleanType()),
                T.StructField("score", T.IntegerType()),
            ]
        ),
    )

    rule = CaseWhenRule(
        branches=[
            WhenThenConditional("`A` = TRUE", ThenAction(then_expr="`score` + 1")),
        ],
        default_case=DefaultCase(then=ThenAction(then_expr="NULL")),
        __seed=7,
        __row_idx=F.col("__row_idx"),
        __table="t",
        __column="B",
    )

    runtime = MockRuntime()
    output = rule.apply(
        runtime=cast(Any, runtime), base_df=parent_col_df, target_col="B"
    )
    vals = [r["B"] for r in output.select("B").collect()]
    assert vals[0] == 11 and vals[1] is None


def test_four_branches_order_and_exclusivity(spark):
    rows = [("critical",), ("high",), ("medium",), ("low",), ("unknown",)]
    parent_col_df = create_test_parent_col(
        spark, rows, T.StructType([T.StructField("warning_level", T.StringType())])
    )

    rule = CaseWhenRule(
        branches=[
            WhenThenConditional(
                "`warning_level` = 'critical'", ThenAction(then_expr="'Level_1'")
            ),
            WhenThenConditional(
                "`warning_level` = 'high'", ThenAction(then_expr="'Level_2'")
            ),
            WhenThenConditional(
                "`warning_level` = 'medium'", ThenAction(then_expr="'Level_3'")
            ),
            WhenThenConditional(
                "`warning_level` = 'low'", ThenAction(then_expr="'Level_4'")
            ),
        ],
        default_case=DefaultCase(then=ThenAction(then_expr="'No_Level'")),
        __seed=99,
        __row_idx=F.col("__row_idx"),
        __table="t",
        __column="penalty_band",
    )

    runtime = MockRuntime()
    output = rule.apply(
        runtime=cast(Any, runtime), base_df=parent_col_df, target_col="penalty_band"
    )

    vals = [
        r["penalty_band"]
        for r in output.orderBy("warning_level").select("penalty_band").collect()
    ]
    assert vals == ["Level_1", "Level_2", "Level_4", "Level_3", "No_Level"]


def test_validation_then_action_xor_both_none_raises():
    action = ThenAction(then_expr=None, then_rule=None)
    with pytest.raises(ValueError):
        action.validate_action()


def test_validation_then_action_xor_both_set_raises():
    action = ThenAction(then_expr="1", then_rule="const_literal")
    with pytest.raises(ValueError):
        action.validate_action()


def test_default_case_applies_when_no_match(spark):
    rows = [(False,), (False,)]
    parent_col_df = create_test_parent_col(
        spark,
        rows,
        T.StructType([T.StructField("is_current_employee", T.BooleanType())]),
    )

    rule = CaseWhenRule(
        branches=[
            WhenThenConditional(
                "`is_current_employee` = TRUE", ThenAction(then_expr="'some_string'")
            )
        ],
        default_case=DefaultCase(then=ThenAction(then_expr="NULL")),
        __seed=1,
        __row_idx=F.col("__row_idx"),
        __table="t",
        __column="child_col",
    )

    runtime = MockRuntime()
    output = rule.apply(
        runtime=cast(Any, runtime), base_df=parent_col_df, target_col="child_col"
    )
    assert [r["child_col"] for r in output.select("child_col").collect()] == [
        None,
        None,
    ]
