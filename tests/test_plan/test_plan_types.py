import pytest

from respark.rules.registry import get_generation_rule
from respark.rules.generation_rules.generate_string import RandomStringRule
from respark.plan import (
    SchemaGenerationPlan,
    TableGenerationPlan,
    ColumnGenerationPlan,
)


def wave_index(layers, name: str) -> int:
    for i, wave in enumerate(layers):
        if name in wave:
            return i
    raise AssertionError(f"{name} not found in layers {layers}")


def test_add_fk_constraint_and_duplicate_error():
    plan = SchemaGenerationPlan()
    name = plan.add_fk_constraint(
        "employees", "employee_id", "appraisals", "employee_id"
    )

    assert name in plan.fk_constraints
    assert plan.table_generation_layers is None

    with pytest.raises(ValueError):
        plan.add_fk_constraint("employees", "employee_id", "appraisals", "employee_id")


def test_remove_fk_constraint_success_and_missing():
    plan = SchemaGenerationPlan()
    name = plan.add_fk_constraint(
        "employees", "employee_id", "appraisals", "employee_id"
    )

    plan.remove_fk_constraint(name)
    assert plan.fk_constraints == {}

    with pytest.raises(KeyError):
        plan.remove_fk_constraint(name)


def test_get_table_plan_and_update_row_count():
    plan = SchemaGenerationPlan(
        table_plans={
            "employees": TableGenerationPlan(name="employees", row_count=10),
            "appraisals": TableGenerationPlan(name="appraisals", row_count=20),
        }
    )

    t1 = plan.get_table_plan("employees")
    assert t1.row_count == 10

    plan.update_table_row_count("employees", 100)
    assert plan.get_table_plan("employees").row_count == 100

    with pytest.raises(ValueError):
        plan.get_table_plan("some_missing_table")

    with pytest.raises(ValueError):
        plan.update_table_row_count("some_missing_table", 100)


def test_get_column_plan():
    plan = SchemaGenerationPlan(
        table_plans={
            "employees": TableGenerationPlan(
                name="employees",
                row_count=200,
                column_plans={
                    "first_name": ColumnGenerationPlan(
                        name="first_name",
                        data_type="string",
                        rule=get_generation_rule(
                            "random_string", **{"min_length": 2, "max_length": 10}
                        ),
                    ),
                    "department_id": ColumnGenerationPlan(
                        name="department_id",
                        data_type="int",
                        rule=get_generation_rule(
                            "sample_from_reference", **{"min_value": 1, "max_value": 10}
                        ),
                    ),
                },
            )
        }
    )

    col = plan.get_column_plan("employees", "first_name")
    assert isinstance(col.rule, RandomStringRule)
    assert col.rule.params == {"min_length": 2, "max_length": 10}


def test_build_table_dag_orders_parents_before_children():
    plan = SchemaGenerationPlan(
        table_plans={
            "employees": TableGenerationPlan(name="employees", row_count=10),
            "appraisals": TableGenerationPlan(name="appraisals", row_count=100),
        }
    )
    plan.add_fk_constraint("employees", "employee_id", "appraisals", "employee_id")

    plan.build_inter_table_dependencies()
    layers = plan.table_generation_layers
    assert isinstance(layers, list)

    assert wave_index(layers, "employees") < wave_index(layers, "appraisals")


def test_build_table_dag_cycle_raises_runtimeerror():
    plan = SchemaGenerationPlan(
        table_plans={
            "employees": TableGenerationPlan(name="employees", row_count=10),
            "appraisals": TableGenerationPlan(name="appraisals", row_count=100),
        }
    )
    plan.add_fk_constraint("employees", "employee_id", "appraisals", "employee_id")
    plan.add_fk_constraint("appraisals", "employee_id", "employees", "employee_id")

    with pytest.raises(RuntimeError) as e:
        plan.build_inter_table_dependencies()
    assert "Cycle detected" in str(e.value)
