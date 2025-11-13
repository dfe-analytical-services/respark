from respark import ResparkRuntime


def test_synthetic_schema_table_names(spark, employees_df, departments_df, sales_df):
    rt = ResparkRuntime(spark)
    rt.register_source("employees", employees_df)
    rt.register_reference("departments", departments_df)
    rt.register_source("sales", sales_df)

    rt.profile_sources()
    generation_plan = rt.create_generation_plan()
    output_synth_dfs = rt.generate()

    expected_names = list(generation_plan.table_plans.keys())
    assert set(output_synth_dfs.keys()) == set(expected_names)
