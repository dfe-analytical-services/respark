from dataclasses import dataclass
from typing import ClassVar, TypedDict, Literal, Optional
from pyspark.sql import DataFrame, functions as F, types as T
from .base_column_profile import BaseColumnProfile, calculate_null_stats


class FractionalParams(TypedDict):
    min_value: Optional[float]
    max_value: Optional[float]
    mean_value: Optional[float]


@dataclass(slots=True)
class FractionalColumnProfile(BaseColumnProfile[FractionalParams]):
    spark_subtype: ClassVar[Literal["float", "double"]]

    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean_value: Optional[float] = None

    def type_specific_params(self) -> FractionalParams:
        return {
            "min_value": self.min_value,
            "max_value": self.max_value,
            "mean_value": self.mean_value,
        }


@dataclass(slots=True)
class FloatColumnProfile(FractionalColumnProfile):
    spark_subtype: str = "float"


@dataclass(slots=True)
class DoubleColumnProfile(FractionalColumnProfile):
    spark_subtype: str = "double"


def profile_fractional_column(
    source_df: DataFrame, col_name: str
) -> FractionalColumnProfile:
    field = source_df.schema[col_name]
    nullable = field.nullable
    null_ratio = (
        calculate_null_stats(source_df=source_df, col_name=col_name)
        if nullable
        else 0.0
    )

    data_type = field.dataType

    if isinstance(data_type, T.FloatType):
        FractionalClass = FloatColumnProfile
        cast_type = "float"
    elif isinstance(data_type, T.DoubleType):
        FractionalClass = DoubleColumnProfile
        cast_type = "double"
    else:
        raise TypeError(f"Column {col_name} is not a fractional type: {data_type}")

    col_profile = (
        source_df.select(F.col(col_name).cast(cast_type).alias("val"))
        .agg(
            F.min("val").alias("min_value"),
            F.max("val").alias("max_value"),
            F.avg("val").alias("mean_value"),
        )
        .first()
    )

    col_stats = col_profile.asDict() if col_profile else {}

    return FractionalClass(
        col_name=col_name,
        nullable=nullable,
        null_ratio=null_ratio,
        min_value=col_stats.get("min_value"),
        max_value=col_stats.get("max_value"),
        mean_value=col_stats.get("mean_value"),
    )
