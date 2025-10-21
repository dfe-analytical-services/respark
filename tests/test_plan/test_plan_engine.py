import json
import pytest

from respark.plan.plan_engine import (
    ColumnGenerationPlan,
    TableGenerationPlan,
    SchemaGenerationPlan,
)


# ---------- Fixtures ----------


@pytest.fixture
def employees_departments_plan() -> SchemaGenerationPlan:
    """
    A sample plan reflecting your provided datasets:

    tables:
      - employees:
          columns: employee_id (long), first_name (string), last_name (string),
                   department_id (int, FK -> departments.department_id),
                   is_current (boolean)
      - departments:
          columns: department_id (int), department_name (string)
    """
    return SchemaGenerationPlan(
        tables=[
            TableGenerationPlan(
                name="employees",
                # You have 10 employee rows in tests/data
                row_count=10,
                columns=[
                    ColumnGenerationPlan(
                        name="employee_id",
                        data_type="long",
                        rule="sequence",
                        params={"start": 100000000, "step": 1},
                    ),
                    ColumnGenerationPlan(
                        name="first_name",
                        data_type="string",
                        rule="faker.first_name",
                        params={},
                    ),
                    ColumnGenerationPlan(
                        name="last_name",
                        data_type="string",
                        rule="faker.last_name",
                        params={},
                    ),
                    ColumnGenerationPlan(
                        name="department_id",
                        data_type="int",
                        rule="foreign_key",
                        params={"references": "departments.department_id"},
                    ),
                    ColumnGenerationPlan(
                        name="is_current",
                        data_type="boolean",
                        rule="choice",
                        params={"values": [True, False], "weights": [0.7, 0.3]},
                    ),
                ],
            ),
            TableGenerationPlan(
                name="departments",
                # You have 5 department rows in tests/data
                row_count=5,
                columns=[
                    ColumnGenerationPlan(
                        name="department_id",
                        data_type="int",
                        rule="sequence",
                        params={"start": 1, "step": 1},
                    ),
                    ColumnGenerationPlan(
                        name="department_name",
                        data_type="string",
                        rule="literal_set",
                        params={
                            "values": [
                                "HR",
                                "Finance",
                                "Engineering",
                                "Marketing",
                                "IT",
                            ]
                        },
                    ),
                ],
            ),
        ]
    )


# ---------- Serialization ----------


def test_schema_to_dict_shape_matches_employees_departments(
    employees_departments_plan: SchemaGenerationPlan,
):
    d = employees_departments_plan.to_dict()
    assert isinstance(d, dict)
    assert set(t["name"] for t in d["tables"]) == {"employees", "departments"}

    employees = next(t for t in d["tables"] if t["name"] == "employees")
    assert employees["row_count"] == 10
    assert [c["name"] for c in employees["columns"]] == [
        "employee_id",
        "first_name",
        "last_name",
        "department_id",
        "is_current",
    ]

    departments = next(t for t in d["tables"] if t["name"] == "departments")
    assert departments["row_count"] == 5
    assert [c["name"] for c in departments["columns"]] == [
        "department_id",
        "department_name",
    ]


def test_schema_to_json_roundtrip(
    tmp_path, employees_departments_plan: SchemaGenerationPlan
):
    path = tmp_path / "plan.json"
    employees_departments_plan.to_json(str(path))

    with path.open() as f:
        data = json.load(f)

    # Compare with to_dict() to avoid ordering issues
    assert data == employees_departments_plan.to_dict()


# ---------- Getters ----------


def test_get_table_plan_success(employees_departments_plan: SchemaGenerationPlan):
    t = employees_departments_plan.get_table_plan("employees")
    assert isinstance(t, TableGenerationPlan)
    assert t.name == "employees"
    assert t.row_count == 10


def test_get_table_plan_not_found(employees_departments_plan: SchemaGenerationPlan):
    with pytest.raises(ValueError) as exc:
        employees_departments_plan.get_table_plan("sales")  # not present here
    assert "not found" in str(exc.value).lower()


def test_get_column_plan_success_fk(employees_departments_plan: SchemaGenerationPlan):
    col = employees_departments_plan.get_column_plan("employees", "department_id")
    assert isinstance(col, ColumnGenerationPlan)
    assert col.name == "department_id"
    assert col.rule == "foreign_key"
    assert col.params == {"references": "departments.department_id"}


def test_get_column_plan_not_found(employees_departments_plan: SchemaGenerationPlan):
    with pytest.raises(ValueError) as exc:
        employees_departments_plan.get_column_plan("employees", "unknown_col")
    assert "not found" in str(exc.value).lower()


# ---------- Updaters ----------


def test_update_column_rule(employees_departments_plan: SchemaGenerationPlan):
    employees_departments_plan.update_column_rule(
        "employees", "first_name", "faker.given_name"
    )
    col = employees_departments_plan.get_column_plan("employees", "first_name")
    assert col.rule == "faker.given_name"


def test_update_column_rule_not_found(employees_departments_plan: SchemaGenerationPlan):
    with pytest.raises(ValueError):
        employees_departments_plan.update_column_rule(
            "employees", "missing_col", "anything"
        )


def test_update_column_params_merges_on_fk(
    employees_departments_plan: SchemaGenerationPlan,
):
    # Pre-check
    col = employees_departments_plan.get_column_plan("employees", "department_id")
    assert col.params == {"references": "departments.department_id"}

    # Merge: override references and add a new key
    employees_departments_plan.update_column_params(
        "employees",
        "department_id",
        {"references": "departments.department_id", "on_delete": "restrict"},
    )

    col = employees_departments_plan.get_column_plan("employees", "department_id")
    assert col.params["references"] == "departments.department_id"
    assert col.params["on_delete"] == "restrict"
    assert set(col.params.keys()) == {"references", "on_delete"}


def test_update_column_params_not_found(
    employees_departments_plan: SchemaGenerationPlan,
):
    with pytest.raises(ValueError):
        employees_departments_plan.update_column_params(
            "departments", "unknown_col", {"x": 1}
        )


def test_update_table_row_count(employees_departments_plan: SchemaGenerationPlan):
    employees_departments_plan.update_table_row_count("employees", 11)
    assert employees_departments_plan.get_table_plan("employees").row_count == 11


def test_update_table_row_count_not_found(
    employees_departments_plan: SchemaGenerationPlan,
):
    with pytest.raises(ValueError):
        employees_departments_plan.update_table_row_count("missing_table", 1)


# ---------- Sanity: getters return live references ----------


def test_getters_return_live_objects(employees_departments_plan: SchemaGenerationPlan):
    t = employees_departments_plan.get_table_plan("departments")
    t.row_count = 6
    assert employees_departments_plan.get_table_plan("departments").row_count == 6
    c = employees_departments_plan.get_column_plan("employees", "employee_id")
    c.rule = "uuid"
    assert (
        employees_departments_plan.get_column_plan("employees", "employee_id").rule
        == "uuid"
    )
