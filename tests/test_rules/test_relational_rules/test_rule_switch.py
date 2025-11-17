import pytest
from pyspark.sql import Column, functions as F
from respark.rules.registry import register_generation_rule
from respark.rules.rule_types import GenerationRule
from respark.rules.relational_rules.rule_switch import RuleSwitch


@register_generation_rule("mock_generation_rule")
class MockGenerationRule(GenerationRule):
    """
    A basic rule for mocking Generation Rules. .
    """

    def generate_column(self) -> Column:
        mock_name = self.params.get("mock_name")
        mock_value = self.params.get("mock_value", None)
        output = f"{mock_name}:{mock_value}"
        return F.lit(output)


def _mock_dataframe(spark):
    return spark.createDataFrame(
        [
            (None, "Current", 10),
            ("2025-01-01", "Not Active", 95),
            ("2020-06-01", "Not Active", 50),
        ],
        ["employment_end_date", "status", "most_recent_appraisal"],
    )


def test_rule_switch_first_match_wins(spark):
    df = _mock_dataframe(spark)

    params = {
        "__seed": 100,
        "branches": [
            {
                "when": "`employment_end_date` IS NULL",
                "then": {
                    "rule_name": "mock_generation_rule",
                    "mock_name": "ruleA",
                    "mock_value": "one",
                },
            },
            {
                "when": "`status` = 'Not Active'",
                "then": {
                    "rule_name": "mock_generation_rule",
                    "mock_name": "ruleB",
                    "mock_value": "two",
                },
            },
        ],
        "default": {
            "rule_name": "mock_generation_rule",
            "mock_name": "default_rule",
            "mock_value": "some_default",
        },
    }

    rule = RuleSwitch(**params)
    col = rule.generate_column()

    out = df.select(col.alias("out")).collect()
    assert out[0]["out"] == "ruleA:one"
    assert out[1]["out"] == "ruleB:two"
    assert out[2]["out"] == "ruleB:two"


def test_rule_switch_default_used_when_no_match(spark):
    df = _mock_dataframe(spark)

    params = {
        "__seed": 7,
        "branches": [
            {
                "when": "`most_recent_appraisal` > 100",
                "then": {
                    "rule_name": "mock_generation_rule",
                    "mock_name": "A",
                    "mock_value": "x",
                },
            },
        ],
        "default": {
            "rule_name": "mock_generation_rule",
            "mock_name": "default_rule",
            "mock_value": "some_default",
        },
    }

    rule = RuleSwitch(**params)
    col = rule.generate_column()
    out = df.select(col.alias("out")).collect()

    assert [r["out"] for r in out] == [
        "default_rule:some_default",
        "default_rule:some_default",
        "default_rule:some_default",
    ]


def test_rule_switch_branch_missing_key_raises():

    params = {
        "__seed": 10,
        "branches": [{"when": "`status` = 'Current'"}],
        "default": {
            "rule_name": "mock_generation_rule",
            "mock_name": "default_rule",
            "mock_value": "some_default",
        },
    }

    rule = RuleSwitch(**params)
    with pytest.raises(KeyError):
        rule.generate_column()


def test_rule_switch_branch_extra_key_raises(spark):
    df = _mock_dataframe(spark)

    params = {
        "__seed": 10,
        "branches": [
            {
                "when": "`status` = 'Current'",
                "then": {
                    "rule_name": "mock_generation_rule",
                    "mock_name": "A",
                    "mock_value": "x",
                },
                "some_extra_key": "some_value",
            }
        ],
        "default": {
            "rule_name": "mock_generation_rule",
            "mock_name": "default_rule",
            "mock_value": "some_default",
        },
    }

    rule = RuleSwitch(**params)
    with pytest.raises(KeyError):
        rule.generate_column()


def test_rule_switch_no_default_raises():

    params = {
        "__seed": 10,
        "branches": [
            {
                "when": "`status` = 'Current'",
                "then": {
                    "rule_name": "mock_generation_rule",
                    "mock_name": "A",
                    "mock_value": "x",
                },
            }
        ],
        # no default_case
    }

    rule = RuleSwitch(**params)
    with pytest.raises(KeyError):
        rule.generate_column()


def test_rule_switch_collect_parent_columns():

    params = {
        "__seed": 1,
        "branches": [
            {
                "when": "`employment_end_date` IS NULL",
                "then": {
                    "rule_name": "mock_generation_rule",
                    "mock_name": "A",
                    "mock_value": "x",
                },
            },
            {
                "when": "`status` = 'ACTIVE'",
                "then": {
                    "rule_name": "mock_generation_rule",
                    "mock_name": "B",
                    "mock_value": "x",
                },
            },
        ],
        "default": {
            "rule_name": "mock_generation_rule",
            "mock_name": "default_rule",
            "mock_value": "`some_parent_col`",
        },
    }

    rule = RuleSwitch(**params)
    cols = rule.collect_parent_columns()

    assert "employment_end_date" in cols
    assert "status" in cols
    assert "some_parent_col" in cols
