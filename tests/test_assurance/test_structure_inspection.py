from respark import ResparkRuntime
from respark.layer_assurance.inspect_structure import (
    SchemaStructureCheck,
    SchemaStructureParams,
)


def test_SchemaStructureCheck_passes_valid_schemas(spark,
    employees_df, departments_df, sales_df
):
    mock_runtime = ResparkRuntime(spark)
    mock_runtime.register_source("employees", employees_df)
    mock_runtime.register_reference("departments", departments_df)
    mock_runtime.register_source("sales", sales_df)
    mock_runtime.profile_sources()
    mock_runtime.create_generation_plan()

    synth_data = mock_runtime.generate()

    params = SchemaStructureParams(
        source_data=mock_runtime.sources, synth_data=synth_data
    )
    schema_inspection = SchemaStructureCheck(params)
    result = schema_inspection.inspect()

    assert result.status == "Passed"
    assert result.message == "OK"
    assert result.name == "check_schema_structure"
    assert result.theme == "Structure"


def test_SchemaStructureCheck_fails_invalid_schemas(spark,
    employees_df, departments_df, sales_df
):
    mock_runtime = ResparkRuntime(spark)
    mock_runtime.register_source("employees", employees_df)
    mock_runtime.register_reference("departments", departments_df)
    mock_runtime.register_source("sales", sales_df)
    mock_runtime.profile_sources()
    mock_runtime.create_generation_plan()

    synth_data = mock_runtime.generate()

    mock_runtime.sources["not_employees"] = mock_runtime.sources["employees"]
    mock_runtime.sources.pop("employees")

    params = SchemaStructureParams(
        source_data=mock_runtime.sources, synth_data=synth_data
    )
    schema_inspection = SchemaStructureCheck(params)
    result = schema_inspection.inspect()

    assert result.status == "Failed"
    assert result.message == "Different tables between source and synthetic data"
    assert result.name == "check_schema_structure"
    assert result.theme == "Structure"
