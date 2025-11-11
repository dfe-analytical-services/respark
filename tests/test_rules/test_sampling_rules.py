import pytest
from typing import Any, cast
from pyspark.sql import functions as F
from pyspark.sql import types as T
from respark.rules.relational_rules.sampling_rules import SampleFromReference, ForeignKeyFromParent
from respark.rules import GENERATION_RULES_REGISTRY


class MockRuntime:
    """
    A simple mock runtime with the attributes used by the rules.
    """

    def __init__(self, references=None, generated_synthetics=None):
        self.references = references or {}
        self.generated_synthetics = generated_synthetics or {}


def test_rules_are_registered_under_new_names():
    assert "sample_from_reference" in GENERATION_RULES_REGISTRY
    assert "fk_from_parent" in GENERATION_RULES_REGISTRY


###
# Testing SampleFromReference
###


def test_sample_from_reference_happy_path(employees_df, departments_df, test_seed):
    rule = SampleFromReference(
        reference_name="departments",
        column="department_id",
        __seed=test_seed,
        __row_idx=F.monotonically_increasing_id(),
        __table="employees",
    )

    runtime = MockRuntime(references={"departments": departments_df})

    out = rule.apply(
        df=employees_df,
        runtime=cast(Any, runtime),
        target_col="department_id",
    )

    assert out.count() == employees_df.count()
    assert out.schema["department_id"].dataType == T.IntegerType()

    ref_ids = {
        r["department_id"]
        for r in departments_df.select("department_id").distinct().collect()
    }
    sampled_ids = {
        r["department_id"] for r in out.select("department_id").distinct().collect()
    }
    assert sampled_ids.issubset(ref_ids)
    assert len(sampled_ids) >= 1


def test_sample_from_reference_missing_runtime_raises(employees_df, test_seed):
    rule = SampleFromReference(
        reference_name="departments",
        column="department_id",
        __seed=test_seed,
        __row_idx=F.lit(0),
        __table="employees",
    )
    with pytest.raises(RuntimeError):
        rule.apply(employees_df, runtime=None, target_col="department_id")


def test_sample_from_reference_missing_reference_key_raises(
    employees_df, departments_df, test_seed
):
    rule = SampleFromReference(
        reference_name="missing_ref",
        column="department_id",
        __seed=test_seed,
        __row_idx=F.lit(0),
        __table="employees",
    )
    runtime = MockRuntime(references={"departments": departments_df})
    with pytest.raises(ValueError) as exc:
        rule.apply(employees_df, runtime=cast(Any, runtime), target_col="department_id")
    assert "not found" in str(exc.value).lower()


def test_sample_from_reference_missing_params_raises_value_error(
    employees_df, test_seed
):
    rule = SampleFromReference(
        __seed=test_seed,
        __row_idx=F.lit(0),
        __table="employees",
    )
    runtime = MockRuntime(references={})

    with pytest.raises(KeyError):
        rule.apply(
            df=employees_df,
            runtime=cast(Any, runtime),
            target_col="department_id",
        )
