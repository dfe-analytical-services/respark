from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar, TypedDict, Literal, Optional, cast
from pyspark.sql import DataFrame, functions as F, types as T
from .base_profile import BaseColumnProfile


# Parameters unique to Decimal values
class DecimalParams(TypedDict):
    precision: int
    scale: int
    min_value: Optional[str]
    max_value: Optional[str]
    mean_value: Optional[float]


# Decimal Column Profile Class
@dataclass(slots=True)
class DecimalColumnProfile(BaseColumnProfile[DecimalParams]):

    spark_subtype: ClassVar[Literal["decimal"]] = "decimal"
    precision: int
    scale: int
    min_value: Optional[Decimal] = None
    max_value: Optional[Decimal] = None
    mean_value: Optional[float] = None

    def default_rule(self) -> str:
        return "random_decimal"

    def type_specific_params(self) -> DecimalParams:
        return {
            "precision": self.precision,
            "scale": self.scale,
            "min_value": str(self.min_value) if self.min_value is not None else None,
            "max_value": str(self.max_value) if self.max_value is not None else None,
            "mean_value": self.mean_value,
        }


def profile_decimal_column(df: DataFrame, col_name: str) -> DecimalColumnProfile:
    field = df.schema[col_name]
    data_type = field.dataType

    if not isinstance(data_type, T.DecimalType):
        raise TypeError(f"Column {col_name} is not DecimalType; got {data_type}")

    dec_data_type = cast(T.DecimalType, data_type)

    nullable = field.nullable
    spark_subtype = "decimal"
    precision = dec_data_type.precision
    scale = dec_data_type.scale

    col_profile = (
        df.select(F.col(col_name).alias("val"))
        .agg(
            F.min("val").alias("min_value"),
            F.max("val").alias("max_value"),
            F.avg(F.col("val").cast("double")).alias("mean_value"),
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
    mean_value: Optional[float] = col_stats.get("mean_value")

    return DecimalColumnProfile(
        name=col_name,
        normalised_type="numeric",
        nullable=nullable,
        precision=precision,
        scale=scale,
        min_value=min_value,
        max_value=max_value,
        mean_value=mean_value,
    )
