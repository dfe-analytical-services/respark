from respark.profiling import SchemaProfiler
from respark.planning import SchemaGenerationPlan, make_generation_plan


def test_generation_plan_infers_rules_and_params(employees_df, departments_df):

    # Generate test profile and proposed generation plan
    profiler = SchemaProfiler()
    schema_profile = profiler.profile_schema(
        {
            "employees": employees_df,
            "departments": departments_df,
        }
    )
    plan = make_generation_plan(schema_profile)

    # Check Generation Plan
    assert isinstance(plan, SchemaGenerationPlan)
    tables = {t.name: t for t in plan.tables}
    assert set(tables) == {"employees", "departments"}
    assert tables["employees"].row_count == employees_df.count()
    assert tables["departments"].row_count == departments_df.count()

    emp_cols = {c.name: c for c in tables["employees"].columns}
    dep_cols = {c.name: c for c in tables["departments"].columns}

    # Check default assigned rules
    assert {c.name: c.rule for c in tables["employees"].columns} == {
        "first_name": "random_string",
        "last_name": "random_string",
        "department_id": "random_int",
        "start_date": "random_date",
        "salary": "random_int",
    }
    assert {c.name: c.rule for c in tables["departments"].columns} == {
        "department_id": "random_int",
        "department_name": "random_string",
    }

    # Check parameters passed as expected
    assert emp_cols["first_name"].params.get("min_length") == 3
    assert emp_cols["first_name"].params.get("max_length") == 6

    assert emp_cols["last_name"].params.get("min_length") == 4
    assert emp_cols["last_name"].params.get("max_length") == 8

    assert dep_cols["department_name"].params.get("min_length") == 2
    assert dep_cols["department_name"].params.get("max_length") == 11

    assert emp_cols["department_id"].params.get("min_value") == 1
    assert emp_cols["department_id"].params.get("max_value") == 5

    assert emp_cols["salary"].params.get("min_value") == 29500
    assert emp_cols["salary"].params.get("max_value") == 56000

    assert dep_cols["department_id"].params.get("min_value") == 1
    assert dep_cols["department_id"].params.get("max_value") == 5

    assert emp_cols["start_date"].params.get("min_date") == "2020-02-01"
    assert emp_cols["start_date"].params.get("max_date") == "2020-02-10"

    # Test .to_dict()
    as_dict = plan.to_dict()
    assert isinstance(as_dict, dict)
    assert "tables" in as_dict and isinstance(as_dict["tables"], list)
