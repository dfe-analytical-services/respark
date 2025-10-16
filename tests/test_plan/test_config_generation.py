from respark.plan import SchemaGenerationPlan
from respark import ResparkRuntime


def test_generation_plan_infers_rules_and_params(
    spark, employees_df, departments_df, sales_df
):
    mock_runtime = ResparkRuntime(spark)
    mock_runtime.register_source("employees", employees_df)
    mock_runtime.register_reference("departments", departments_df)
    mock_runtime.register_source("sales", sales_df)

    mock_runtime.profile_sources()

    plan = mock_runtime.create_generation_plan()

    # Check Generation Plan
    assert isinstance(plan, SchemaGenerationPlan)
    tables = {t.name: t for t in plan.tables}
    assert set(tables) == {"employees", "sales"}
    assert tables["employees"].row_count == employees_df.count()
    assert tables["sales"].row_count == sales_df.count()

    emp_cols = {c.name: c for c in tables["employees"].columns}

    # Check default assigned rules
    assert {c.name: c.rule for c in tables["employees"].columns} == {
        "employee_id": "random_long",
        "first_name": "random_string",
        "last_name": "random_string",
        "department_id": "random_int",
        "is_current": "random_boolean",
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

    # Test .to_dict()
    as_dict = plan.to_dict()
    assert isinstance(as_dict, dict)
    assert "tables" in as_dict and isinstance(as_dict["tables"], list)
