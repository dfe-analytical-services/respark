from dataclasses import dataclass
from typing import Dict, TypedDict, Literal, Any, Optional
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
    spark_subtype: Literal["byte", "short", "int", "long"] = "int"

    def default_rule(self) -> str:
        return "random_int"

    def type_specific_params(self) -> Dict[str, Any]:
        return {
            "min_value": self.min_value,
            "max_value": self.max_value,
            "mean_value": self.mean_value,
        }


def profile_integral_column(df: DataFrame, col_name: str) -> IntegralColumnProfile:
    field = df.schema[col_name]
    nullable = field.nullable
    data_type = field.dataType

    match data_type:
        case T.ByteType():
            spark_subtype = "byte"
        case T.ShortType():
            spark_subtype = "short"
        case T.IntegerType():
            spark_subtype = "int"
        case T.LongType():
            spark_subtype = "long"
        case _:
            raise TypeError(f"Column {col_name} is not an integral type: {data_type}")

    col_profile = (
        df.select(F.col(col_name).alias("val")).agg(
            F.min("val").alias("min_value"),
            F.max("val").alias("max_value"),
            F.avg("val").alias("mean_value"),
        )
    ).first()

    col_stats = col_profile.asDict() if col_profile else {}

    return IntegralColumnProfile(
        name=col_name,
        normalised_type="numeric",
        nullable=nullable,
        spark_subtype=spark_subtype,
        min_value=col_stats.get("min_value"),
        max_value=col_stats.get("max_value"),
        mean_value=col_stats.get("mean_value"),
    )
