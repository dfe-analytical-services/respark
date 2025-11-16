from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar, TypedDict, Literal, Optional
from pyspark.sql import DataFrame, functions as F, types as T
from .base_column_profile import BaseColumnProfile, calculate_null_stats


class DecimalParams(TypedDict):
    precision: Optional[int]
    scale: Optional[int]
    min_value: Optional[Decimal]
    max_value: Optional[Decimal]


@dataclass(slots=True)
class DecimalColumnProfile(BaseColumnProfile[DecimalParams]):
    spark_subtype: str = "decimal"
    precision: Optional[int] = None
    scale: Optional[int] = None
    min_value: Optional[Decimal] = None
    max_value: Optional[Decimal] = None

    def type_specific_params(self) -> DecimalParams:
        return {
            "precision": self.precision,
            "scale": self.scale,
            "min_value": self.min_value,
            "max_value": self.max_value,
        }


def profile_decimal_column(source_df: DataFrame, col_name: str) -> DecimalColumnProfile:
    field = source_df.schema[col_name]
    nullable = field.nullable
    null_ratio = (
        calculate_null_stats(source_df=source_df, col_name=col_name)
        if nullable
        else 0.0
    )

    if not isinstance(field.dataType, T.DecimalType):
        raise TypeError(f"Column {col_name} is not DecimalType")

    else:
        precision = field.dataType.precision
        scale = field.dataType.scale

    col_profile = (
        source_df.select(F.col(col_name).alias("val"))
        .agg(
            F.min("val").alias("min_value"),
            F.max("val").alias("max_value"),
        )
        .first()
    )

    col_stats = col_profile.asDict() if col_profile else {}

    min_value: Optional[Decimal] = (
        Decimal(str(col_stats["min_value"]))
        if col_stats.get("min_value") is not None
        else None
    )
    max_value: Optional[Decimal] = (
        Decimal(str(col_stats["max_value"]))
        if col_stats.get("max_value") is not None
        else None
    )

    return DecimalColumnProfile(
        col_name=col_name,
        nullable=nullable,
        null_ratio=null_ratio,
        precision=precision,
        scale=scale,
        min_value=min_value,
        max_value=max_value,
    )
