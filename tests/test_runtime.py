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
# Testing adding fk_constraints
###


def test_add_list_remove_fk_constraints(test_runtime, employees_df, sales_df):
    test_runtime.register_source("employees", employees_df)
    test_runtime.register_source("sales", sales_df)

    # Test add:
    fk_name = test_runtime.add_fk_constraint(
        "employees", "employee_id", "sales", "employee_id"
    )
    constraints = test_runtime.list_fk_constraints()
    assert any(c.name == fk_name for c in constraints)

    # Test raising on duplicate add:
    with pytest.raises(ValueError):
        test_runtime.add_fk_constraint(
            "employees", "employee_id", "sales", "employee_id"
        )

    # Test remove:
    test_runtime.remove_fk_constraint(fk_name)
    assert all(c.name != fk_name for c in test_runtime.list_fk_constraints())

    # Test raising on removing again
    with pytest.raises(KeyError):
        test_runtime.remove_fk_constraint(fk_name)


def test_list_fk_constraints_is_sorted_by_name(test_runtime, employees_df, sales_df):
    test_runtime.register_source("employees", employees_df)
    test_runtime.register_source("sales", sales_df)

    added_fk = test_runtime.add_fk_constraint(
        "employees", "employee_id", "sales", "employee_id"
    )
    names = [c.name for c in test_runtime.list_fk_constraints()]

    assert set(names) == {added_fk}


###
# Testing planning
###


def test_create_generation_plan_requires_profile(test_runtime, employees_df, sales_df):
    test_runtime.register_source("employees", employees_df)
    test_runtime.register_source("sales", sales_df)

    with pytest.raises(RuntimeError):
        test_runtime.create_generation_plan()


def test_create_generation_plan_and_layers_happy_path(
    test_runtime, employees_df, sales_df
):
    test_runtime.register_source("employees", employees_df)
    test_runtime.register_source("sales", sales_df)
    test_runtime.profile_sources()

    test_runtime.add_fk_constraint("employees", "employee_id", "sales", "employee_id")

    plan = test_runtime.create_generation_plan()
    assert plan is not None
    plan_table_names = {t.name for t in plan.tables}
    assert {"employees", "sales"}.issubset(plan_table_names)

    layers = test_runtime.get_generation_layers()
    assert isinstance(layers, list)
    assert any("sales" in layer for layer in layers)
    assert any("employees" in layer for layer in layers)

    # Check pk -> fk parent/child layer order is respected
    index_by_table = {tbl: i for i, layer in enumerate(layers) for tbl in layer}
    assert index_by_table["employees"] <= index_by_table["sales"]


def test_plan_with_constraint_to_unknown_table_raises(test_runtime, employees_df):
    test_runtime.register_source("employees", employees_df)
    test_runtime.profile_sources()

    test_runtime.add_fk_constraint("employees", "employee_id", "sales", "employee_id")

    with pytest.raises(ValueError) as e:
        test_runtime.create_generation_plan()
    assert "Constraints reference tables outside the plan" in str(e.value)


def test_cycle_in_fk_raises_runtime_error(test_runtime, employees_df, sales_df):
    test_runtime.register_source("employees", employees_df)
    test_runtime.register_source("sales", sales_df)
    test_runtime.profile_sources()

    # Induce a cycle
    test_runtime.add_fk_constraint("employees", "employee_id", "sales", "employee_id")
    test_runtime.add_fk_constraint("sales", "employee_id", "employees", "employee_id")

    with pytest.raises(RuntimeError) as e:
        test_runtime.create_generation_plan()
    assert "Cycle detected" in str(e.value)


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

    calls = {}

    class FakeSynthSchemaGenerator:
        def __init__(self, spark, references, runtime):
            calls["init"] = {"references": references, "runtime": runtime}

        def generate_synthetic_schema(self, schema_gen_plan, fk_constraints):
            calls["args"] = (schema_gen_plan, fk_constraints)

            return {"employees": test_runtime.sources["employees"]}

    monkeypatch.setattr(
        "respark.runtime.SynthSchemaGenerator", FakeSynthSchemaGenerator
    )

    output = test_runtime.generate()

    # Test that the generator was passed a generation plan, but no fk constraints
    assert calls["args"][0] is test_runtime.generation_plan
    assert isinstance(calls["args"][1], list)
    assert calls["args"][1] == []

    # Test that the generator was initialised with references and a runtime
    assert calls["init"]["references"] is test_runtime.references
    assert calls["init"]["runtime"] is test_runtime

    # Test that a table was generated
    assert "employees" in output
    assert isinstance(output["employees"], DataFrame)
