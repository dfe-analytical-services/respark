from dataclasses import dataclass
from typing import Dict, TypedDict, Literal, Any, Optional
from pyspark.sql import DataFrame, functions as F, types as T
from .base_profile import BaseColumnProfile


# Parameters unique to String values
class StringParams(TypedDict):
    min_length: Optional[int]
    max_length: Optional[int]
    mean_length: Optional[float]


# String Column Profile Class
@dataclass(slots=True)
class StringColumnProfile(BaseColumnProfile):
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    mean_length: Optional[float] = None
    spark_subtype: Literal["string"] = "string"

    def default_rule(self) -> str:
        return "random_string"

    def type_specific_params(self) -> Dict[str, Any]:
        return {
            "min_length": self.min_length,
            "max_length": self.max_length,
            "mean_length": self.mean_length,
        }


def profile_string_column(df: DataFrame, col_name: str) -> StringColumnProfile:
    field = df.schema[col_name]
    nullable = field.nullable
    spark_subtype = "string"

    length_col = F.length(F.col(col_name))

    col_profile = (
        df.select(length_col.alias("len")).agg(
            F.min("len").alias("min_length"),
            F.max("len").alias("max_length"),
            F.avg("len").alias("mean_length"),
        )
    ).first()

    col_stats = col_profile.asDict() if col_profile else {}

    return StringColumnProfile(
        name=col_name,
        normalised_type="string",
        nullable=nullable,
        spark_subtype=spark_subtype,
        min_length=col_stats.get("min_length"),
        max_length=col_stats.get("max_length"),
        mean_length=col_stats.get("mean_length"),
    )
