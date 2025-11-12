import pytest
from typing import Any, cast
from respark.rules.registry import GENERATION_RULES_REGISTRY
from respark.rules.relational_rules.row_expression_rules import RowExpressionRule
from pyspark.sql import functions as F, types as T


def test_rules_are_registered_under_new_names():
    assert "row_based_calculation" in GENERATION_RULES_REGISTRY


class MockRuntime:
    """
    A simple mock runtime with the attributes used by the rules.
    """

    def __init__(self, references=None, generated_synthetics=None):
        self.references = references or {}
        self.generated_synthetics = generated_synthetics or {}


def test_dervied_from_one_column(sales_df, test_seed):
    rule = RowExpressionRule(
        sql_expression="`delivery_distance_miles` * 1.609",
        __seed=test_seed,
        __row_idx=F.lit(0),
        __table="sales",
    )

    runtime = MockRuntime()

    output = rule.apply(
        base_df=sales_df, runtime=cast(Any, runtime), target_col="delivery_distance_km"
    )

    for row in output.collect():
        assert row["delivery_distance_miles"] * 1.609 == pytest.approx(
            row["delivery_distance_km"]
        )


def test_derived_from_multiple_columns(employees_df, test_seed):
    rule = RowExpressionRule(
        sql_expression="CONCAT(`first_name`, ' ', `last_name`)",
        __seed=test_seed,
        __row_idx=F.lit(0),
        __table="employees",
    )

    runtime = MockRuntime()

    output = rule.apply(
        base_df=employees_df, runtime=cast(Any, runtime), target_col="full_name"
    )

    for row in output.collect():
        assert row["first_name"] + " " + row["last_name"] == row["full_name"]
