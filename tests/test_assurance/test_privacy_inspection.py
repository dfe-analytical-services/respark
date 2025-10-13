from datetime import date
from pyspark.sql import Row


from respark.layer_assurance.inspect_privacy import ColumnMatchesCheck, PrivacyParams


def test_ColumnMatchesCheck_passes_valid_schemas(
    employees_df, departments_df, mock_synth_schema
):
    source_data = {"employees": employees_df, "departments": departments_df}
    synth_data = mock_synth_schema

    sensitive_columns = {
        "employees": ["first_name", "last_name"],
    }

    params = PrivacyParams(
        source_data=source_data,
        synth_data=synth_data,
        sensitive_columns=sensitive_columns,
    )
    columns_inspection = ColumnMatchesCheck(params)
    result = columns_inspection.inspect()

    assert result.status == "Passed"
    assert result.message == "OK"
    assert result.name == "check_column_level_matches"
    assert result.theme == "Privacy"


def test_ColumnMatchesCheck_fails_invalid_schemas(
    spark, employees_df, departments_df, mock_synth_schema
):
    source_data = {"employees": employees_df, "departments": departments_df}
    synth_data = mock_synth_schema

    sensitive_columns = {
        "employees": ["first_name", "last_name"],
    }

    leaked_prod_row = Row(
        employee_id=199756800,
        first_name="Ben",
        last_name="Carter",
        department_id=1,
        is_current=True,
    )

    synth_schema = synth_data["employees"].schema
    synth_rows = synth_data["employees"].collect()
    synth_rows[0] = leaked_prod_row
    bad_employees_data = spark.createDataFrame(synth_rows, schema=synth_schema)
    synth_data["employees"] = bad_employees_data

    params = PrivacyParams(
        source_data=source_data,
        synth_data=synth_data,
        sensitive_columns=sensitive_columns,
    )
    columns_inspection = ColumnMatchesCheck(params)
    result = columns_inspection.inspect()

    assert result.status == "Failed"
    assert result.message == "Sensitive value match detected in employees.first_name"
    assert result.name == "check_column_level_matches"
    assert result.theme == "Privacy"
