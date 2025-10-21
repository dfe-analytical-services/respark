from datetime import date
from pyspark.sql import Row

from respark import ResparkRuntime
from respark.layer_assurance.inspect_privacy import ColumnMatchesCheck, PrivacyParams


def test_ColumnMatchesCheck_passes_valid_schemas(
    spark, employees_df, departments_df, sales_df
):
    mock_runtime = ResparkRuntime(spark)
    mock_runtime.register_source("employees", employees_df)
    mock_runtime.register_reference("departments", departments_df)
    mock_runtime.register_source("sales", sales_df)
    mock_runtime.profile_sources()
    mock_runtime.create_generation_plan()

    synth_data = mock_runtime.generate()

    sensitive_columns = {
        "employees": ["first_name", "last_name"],
    }

    params = PrivacyParams(
        source_data=mock_runtime.sources,
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
    spark, employees_df, departments_df, sales_df
):
    mock_runtime = ResparkRuntime(spark)
    mock_runtime.register_source("employees", employees_df)
    mock_runtime.register_reference("departments", departments_df)
    mock_runtime.register_source("sales", sales_df)
    mock_runtime.profile_sources()
    mock_runtime.create_generation_plan()

    synth_data = mock_runtime.generate()

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
        source_data=mock_runtime.sources,
        synth_data=synth_data,
        sensitive_columns=sensitive_columns,
    )

    columns_inspection = ColumnMatchesCheck(params)
    result = columns_inspection.inspect()

    assert result.status == "Failed"
    assert result.message == "Sensitive value match detected in employees.first_name"
    assert result.name == "check_column_level_matches"
    assert result.theme == "Privacy"
