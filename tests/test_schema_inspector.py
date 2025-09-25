import json
import pytest
from respark.schema_inspector import SchemaInspector, SchemaModel

def test_accepts_single_df(spark):
    df = spark.createDataFrame([(1, "Adam")], "id INT, first_name STRING")
    inspector = SchemaInspector(default_single_name="employees")
    model = inspector.inspect(df)

    assert isinstance(model, SchemaModel)
    assert set(model.tables.keys()) == {"employees"}

    cols = model.tables["employees"].columns
    assert set(cols.keys()) == {"id", "first_name"}
    assert cols["id"].normalised_type == "int"
    assert cols["first_name"].normalised_type == "string"
    assert cols["id"].supported is True
    assert cols["first_name"].supported is True