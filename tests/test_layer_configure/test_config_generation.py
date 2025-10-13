from respark.layer_profile import SchemaProfiler
from respark.layer_configure import SchemaGenerationPlan, make_generation_plan


def test_generation_plan_infers_rules_and_params(employees_df, departments_df, sales_df):

    # Generate test profile and proposed generation plan
    profiler = SchemaProfiler()
    schema_profile = profiler.profile_schema(
        {
            "employees": employees_df,
            "departments": departments_df,
            "sales" : sales_df
        }
    )
    plan = make_generation_plan(schema_profile)

    # Check Generation Plan
    assert isinstance(plan, SchemaGenerationPlan)
    tables = {t.name: t for t in plan.tables}
    assert set(tables) == {"employees", "departments", "sales"}
    assert tables["employees"].row_count == employees_df.count()
    assert tables["departments"].row_count == departments_df.count()
    assert tables["sales"].row_count == sales_df.count()

    emp_cols = {c.name: c for c in tables["employees"].columns}
    dep_cols = {c.name: c for c in tables["departments"].columns}
    sales_cols = {c.name: c for c in tables["sales"].columns}

    # Check default assigned rules
    assert {c.name: c.rule for c in tables["employees"].columns} == {
        "employee_id": "random_long",
        "first_name": "random_string",
        "last_name": "random_string",
        "department_id": "random_int",
        "is_current": "random_boolean",
    }

    assert {c.name: c.rule for c in tables["departments"].columns} == {
        "department_id": "random_int",
        "department_name": "random_string",
    }

    assert {c.name: c.rule for c in tables["sales"].columns} == {
        "sale_id": "random_long",
        "sale_date": "random_date",
        "sale_total_gbp": "random_decimal",
        "delivery_distance_miles": "random_double",
        "customer_feedback_score": "random_float",
    }
    

    # Check String Params
    assert emp_cols["first_name"].params.get("min_length") == 3
    assert emp_cols["first_name"].params.get("max_length") == 6


    assert dep_cols["department_id"].params.get("min_value") == 1
    assert dep_cols["department_id"].params.get("max_value") == 5


    # Test .to_dict()
    as_dict = plan.to_dict()
    assert isinstance(as_dict, dict)
    assert "tables" in as_dict and isinstance(as_dict["tables"], list)
