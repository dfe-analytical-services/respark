from respark.layer_assurance.inspect_structure import (
    SchemaStructureCheck,
    SchemaStructureParams,
)


def test_SchemaStructureCheck_passes_valid_schemas(
    employees_df, departments_df, sales_df, mock_synth_schema
):
    source_data = {
        "employees": employees_df,
        "departments": departments_df,
        "sales": sales_df,
    }
    synth_data = mock_synth_schema

    params = SchemaStructureParams(source_data=source_data, synth_data=synth_data)
    schema_inspection = SchemaStructureCheck(params)
    result = schema_inspection.inspect()

    assert result.status == "Passed"
    assert result.message == "OK"
    assert result.name == "check_schema_structure"
    assert result.theme == "Structure"


def test_SchemaStructureCheck_fails_invalid_schemas(
    employees_df, departments_df, mock_synth_schema
):
    source_data = {"not_employees": employees_df, "departments": departments_df}
    synth_data = mock_synth_schema

    params = SchemaStructureParams(source_data=source_data, synth_data=synth_data)
    schema_inspection = SchemaStructureCheck(params)
    result = schema_inspection.inspect()

    assert result.status == "Failed"
    assert result.message == "Different tables between source and synthetic data"
    assert result.name == "check_schema_structure"
    assert result.theme == "Structure"
