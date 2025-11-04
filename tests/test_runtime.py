import pytest
from pyspark.sql import DataFrame

###
# Testing registation & profiling
###


def test_register_source_and_reference(test_runtime, employees_df, departments_df):
    test_runtime.register_source("employees", employees_df)
    test_runtime.register_reference("departments", departments_df)

    assert "employees" in test_runtime.sources
    assert "departments" in test_runtime.references


def test_profile_sources_includes_registered_tables(
    test_runtime, employees_df, sales_df
):
    test_runtime.register_source("employees", employees_df)
    test_runtime.register_source("sales", sales_df)

    profile = test_runtime.profile_sources()
    assert profile is not None
    assert "employees" in profile.tables
    assert "sales" in profile.tables


def test_profile_specific_subset(test_runtime, employees_df, sales_df, departments_df):
    test_runtime.register_source("employees", employees_df)
    test_runtime.register_source("sales", sales_df)
    test_runtime.register_reference("departments", departments_df)

    profile = test_runtime.profile_sources(target_sources=["employees"])
    assert "employees" in profile.tables

    assert "sales" not in profile.tables
    assert "departments" not in profile.tables


###
# Testing update methods
###


def test_update_methods_require_plan(test_runtime):
    with pytest.raises(RuntimeError):
        test_runtime.update_column_rule("employees", "employee_id", "some_rule")

    with pytest.raises(RuntimeError):
        test_runtime.update_column_params("employees", "employee_id", {"k": "v"})

    with pytest.raises(RuntimeError):
        test_runtime.update_table_row_count("employees", 100)


def test_generate_requires_plan(test_runtime):
    with pytest.raises(RuntimeError):
        test_runtime.generate()


def test_generate_uses_synth_schema_generator(monkeypatch, test_runtime, employees_df):
    test_runtime.register_source("employees", employees_df)
    test_runtime.profile_sources()
    test_runtime.create_generation_plan()
    test_runtime.generation_plan.build_inter_table_dependencies()

    calls = {}

    class FakeSynthSchemaGenerator:
        def __init__(self, spark, references=None, runtime=None, **kwargs):
            calls["init"] = {
                "references": references,
                "runtime": runtime,
                "kwargs": kwargs,
            }

        def generate_synthetic_schema(self, *args, **kwargs):
            plan = args[0] if args else kwargs.get("schema_gen_plan")
            calls["plan"] = plan

            return {"employees": test_runtime.sources["employees"]}

    monkeypatch.setattr(
        "respark.runtime.SynthSchemaGenerator", FakeSynthSchemaGenerator
    )

    output = test_runtime.generate()

    assert calls["plan"] is test_runtime.generation_plan
    assert calls["init"]["references"] is test_runtime.references
    assert calls["init"]["runtime"] is test_runtime
