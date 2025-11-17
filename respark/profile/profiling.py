from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame, types as T

from .column_profiles import (
    BaseColumnProfile,
    profile_boolean_column,
    profile_datetime_column,
    profile_decimal_column,
    profile_fractional_column,
    profile_integral_column,
    profile_string_column,
)


TYPE_PROFILE_DISPATCH = {
    T.BooleanType: profile_boolean_column,
    T.ByteType: profile_integral_column,
    T.DoubleType: profile_fractional_column,
    T.DecimalType: profile_decimal_column,
    T.DateType: profile_datetime_column,
    T.FloatType: profile_fractional_column,
    T.IntegerType: profile_integral_column,
    T.ShortType: profile_integral_column,
    T.LongType: profile_integral_column,
    T.StringType: profile_string_column,
    T.TimestampType: profile_datetime_column,
    T.TimestampNTZType: profile_datetime_column,
}

###
# Table Profiling
###


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

        profiler_fn = TYPE_PROFILE_DISPATCH.get(type(spark_dtype))

        if profiler_fn:
            col_profiles[col_name] = profiler_fn(df, col_name)
        else:
            raise TypeError(
                f"Unsupported column type for '{col_name}': {spark_dtype.typeName()}"
            )

    return TableProfile(name=table_name, row_count=df.count(), columns=col_profiles)


###
# Schema Profiling
###


@dataclass(slots=True)
class SchemaProfile:
    tables: Dict[str, TableProfile] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def profile_schema(
    source_dfs: Optional[Dict[str, DataFrame]],
) -> SchemaProfile:
    if source_dfs is None or source_dfs == {}:
        raise ValueError("No sources are registered")
    table_profiles = {name: profile_table(df, name) for name, df in source_dfs.items()}
    return SchemaProfile(tables=table_profiles)
