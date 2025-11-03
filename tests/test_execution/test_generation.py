from respark import ResparkRuntime


def test_synthetic_schema_table_names(spark, employees_df, departments_df, sales_df):
    mock_runtime = ResparkRuntime(spark)
    mock_runtime.register_source("employees", employees_df)
    mock_runtime.register_reference("departments", departments_df)
    mock_runtime.register_source("sales", sales_df)

    mock_runtime.profile_sources()
    generation_plan = mock_runtime.create_generation_plan()

    output_synth_dfs = mock_runtime.generate()

    expected_tables_gen_plans = [
        table_gen_plan for table_gen_plan in generation_plan.table_plans
    ]

    expected_names = [
        table_gen_plan.name for table_gen_plan in expected_tables_gen_plans
    ]
    assert set(output_synth_dfs.keys()) == set(expected_names)
