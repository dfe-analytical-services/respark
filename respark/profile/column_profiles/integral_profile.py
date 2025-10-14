from dataclasses import dataclass
from typing import Dict, ClassVar, TypedDict, Literal, Any, Optional
from pyspark.sql import DataFrame, functions as F, types as T
from .base_profile import BaseColumnProfile


# Parameters unique to Integral values
class IntegralParams(TypedDict):
    min_value: Optional[int]
    max_value: Optional[int]
    mean_value: Optional[float]
    spark_subtype: Literal["byte", "short", "int", "long"]


# Intergral Column Profile Class
@dataclass(slots=True)
class IntegralColumnProfile(BaseColumnProfile[IntegralParams]):
    min_value: Optional[int] = None
    max_value: Optional[int] = None
    mean_value: Optional[float] = None
    spark_subtype: Optional[Literal["byte", "short", "int", "long"]] = None

    def default_rule(self) -> str:
        return f"random_{self.spark_subtype}"

    def type_specific_params(self) -> Dict[str, Any]:
        return {
            "min_value": self.min_value,
            "max_value": self.max_value,
            "mean_value": self.mean_value,
        }


@dataclass(slots=True)
class ByteColumnProfile(IntegralColumnProfile):
    spark_subtype: ClassVar[Literal["byte"]] = "byte"


@dataclass(slots=True)
class ShortColumnProfile(IntegralColumnProfile):
    spark_subtype: ClassVar[Literal["short"]] = "short"


@dataclass(slots=True)
class IntColumnProfile(IntegralColumnProfile):
    spark_subtype: ClassVar[Literal["int"]] = "int"


@dataclass(slots=True)
class LongColumnProfile(IntegralColumnProfile):
    spark_subtype: ClassVar[Literal["long"]] = "long"


def profile_integral_column(df: DataFrame, col_name: str) -> IntegralColumnProfile:
    field = df.schema[col_name]
    nullable = field.nullable
    data_type = field.dataType

    if isinstance(data_type, T.ByteType):
        IntegralClass = ByteColumnProfile
        cast_type = "byte"
    elif isinstance(data_type, T.ShortType):
        IntegralClass = ShortColumnProfile
        cast_type = "double"
    elif isinstance(data_type, T.IntegerType):
        IntegralClass = IntColumnProfile
        cast_type = "int"
    elif isinstance(data_type, T.LongType):
        IntegralClass = LongColumnProfile
        cast_type = "long"
    else:
        raise TypeError(f"Column {col_name} is not a integral type: {data_type}")

    col_profile = (
        df.select(F.col(col_name).cast(cast_type).alias("val"))
        .agg(
            F.min("val").alias("min_value"),
            F.max("val").alias("max_value"),
            F.avg("val").alias("mean_value"),
        )
        .first()
    )
    col_stats = col_profile.asDict() if col_profile else {}

    return IntegralClass(
        name=col_name,
        normalised_type="numeric",
        nullable=nullable,
        min_value=col_stats.get("min_value"),
        max_value=col_stats.get("max_value"),
        mean_value=col_stats.get("mean_value"),
    )
