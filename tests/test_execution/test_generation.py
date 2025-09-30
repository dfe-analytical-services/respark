import pytest
from pyspark.sql import types as T
from respark.executing import SynthSchemaGenerator
from respark.executing.executor import _str_to_spark_type


def test_str_to_spark_type_valid():
    assert isinstance(_str_to_spark_type("string"), T.StringType)
    assert isinstance(_str_to_spark_type("numeric"), T.IntegerType)
    assert isinstance(_str_to_spark_type("date"), T.DateType)


def test_str_to_spark_type_invalid():
    with pytest.raises(TypeError):
        _str_to_spark_type("array")


def test_synthetic_schema_table_names(spark, mock_schema_gen_plan):
    schema_generator = SynthSchemaGenerator(spark)
    output_synth_dfs = schema_generator.generate_synthetic_schema(
        spark, mock_schema_gen_plan
    )

    expected_tables_gen_plans = [
        table_gen_plan for table_gen_plan in mock_schema_gen_plan.tables
    ]

    expected_names = [
        table_gen_plan.name for table_gen_plan in expected_tables_gen_plans
    ]
    assert set(output_synth_dfs.keys()) == set(expected_names)
