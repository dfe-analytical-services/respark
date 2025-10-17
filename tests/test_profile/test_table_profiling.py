import pytest
from decimal import Decimal
from datetime import date
from pyspark.sql import types as T


from respark.profile.column_profiles import (
    BooleanColumnProfile,
    DateColumnProfile,
    DecimalColumnProfile,
    FloatColumnProfile,
    DoubleColumnProfile,
    IntColumnProfile,
    LongColumnProfile,
    StringColumnProfile,
)


from respark.profile import TableProfile, profile_table


def test_profile_table_happy_path_mixed_types(spark):
    schema = T.StructType(
        [
            T.StructField("bool_col", T.BooleanType(), True),
            T.StructField("date_col", T.DateType(), True),
            T.StructField("dec_col", T.DecimalType(10, 2), True),
            T.StructField("float_col", T.FloatType(), True),
            T.StructField("double_col", T.DoubleType(), True),
            T.StructField("int_col", T.IntegerType(), True),
            T.StructField("long_col", T.LongType(), True),
            T.StructField("str_col", T.StringType(), True),
        ]
    )

    rows = [
        (True, date(2024, 1, 1), Decimal("1.23"), 1.0, 10.5, 1, 10000000000, "a"),
        (False, date(2024, 1, 5), Decimal("4.56"), 2.0, 20.5, 2, 20000000000, "bb"),
        (None, None, None, None, None, None, None, None),
    ]
    df = spark.createDataFrame(rows, schema=schema)

    tp = profile_table(df, table_name="all_types_table")

    assert isinstance(tp, TableProfile)
    assert tp.name == "all_types_table"
    assert tp.row_count == len(rows)
    assert set(tp.columns.keys()) == {
        "bool_col",
        "date_col",
        "dec_col",
        "float_col",
        "double_col",
        "int_col",
        "long_col",
        "str_col",
    }

    assert isinstance(tp.columns["bool_col"], BooleanColumnProfile)
    assert isinstance(tp.columns["date_col"], DateColumnProfile)
    assert isinstance(tp.columns["dec_col"], DecimalColumnProfile)
    assert isinstance(tp.columns["float_col"], FloatColumnProfile)
    assert isinstance(tp.columns["double_col"], DoubleColumnProfile)
    assert isinstance(tp.columns["int_col"], IntColumnProfile)
    assert isinstance(tp.columns["long_col"], LongColumnProfile)
    assert isinstance(tp.columns["str_col"], StringColumnProfile)

    assert tp.columns["str_col"].name == "str_col"
    assert tp.columns["int_col"].name == "int_col"


def test_profile_table_unsupported_type_raises(spark):
    schema = T.StructType(
        [
            T.StructField("some_array_field", T.ArrayType(T.IntegerType()), True),
        ]
    )
    df = spark.createDataFrame([([1, 2, 3],), (None,)], schema=schema)

    with pytest.raises(TypeError):
        profile_table(df, "some_array_field")
