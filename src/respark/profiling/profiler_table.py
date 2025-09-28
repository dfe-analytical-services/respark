from dataclasses import dataclass, field
from typing import Dict
from pyspark.sql import DataFrame, types as T
from .profiler_column import (
    BaseColumnProfile,
    profile_string_column,
    profile_numerical_column,
    profile_date_column,
)

type_dispatch = {
    T.StringType: profile_string_column,
    T.IntegerType: profile_numerical_column,
    T.LongType: profile_numerical_column,
    T.FloatType: profile_numerical_column,
    T.DoubleType: profile_numerical_column,
    T.DateType: profile_date_column
}

@dataclass(slots=True)
class TableProfile:
    name: str
    row_count: int
    columns: Dict[str, BaseColumnProfile] = field(default_factory=dict)


def profile_table(df: DataFrame, table_name: str) -> TableProfile:
    col_profiles = {}

    for field in df.schema.fields:
        col_name = field.name
        spark_dtype = field.dataType

        profiler_fn = type_dispatch.get(type(spark_dtype))

        if profiler_fn:
            col_profiles[col_name] = profiler_fn(df, col_name)
        else:
            raise TypeError(f"Unsupported column type for '{col_name}': {spark_dtype.typeName()}")

    return TableProfile(
        name=table_name,
        row_count=df.count(),
        columns=col_profiles
    )

