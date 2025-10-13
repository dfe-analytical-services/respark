from respark.layer_execute import SynthSchemaGenerator


def test_synthetic_schema_table_names(spark, mock_schema_gen_plan):
    schema_generator = SynthSchemaGenerator(spark)
    output_synth_dfs = schema_generator.generate_synthetic_schema(mock_schema_gen_plan)

    expected_tables_gen_plans = [
        table_gen_plan for table_gen_plan in mock_schema_gen_plan.tables
    ]

    expected_names = [
        table_gen_plan.name for table_gen_plan in expected_tables_gen_plans
    ]
    assert set(output_synth_dfs.keys()) == set(expected_names)
