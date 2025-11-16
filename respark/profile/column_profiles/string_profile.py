from dataclasses import dataclass
from typing import Dict, TypedDict, Literal, Any, Optional
from pyspark.sql import DataFrame, functions as F
from .base_column_profile import BaseColumnProfile, calculate_null_stats


class StringParams(TypedDict):
    min_length: Optional[int]
    max_length: Optional[int]


@dataclass(slots=True)
class StringColumnProfile(BaseColumnProfile[StringParams]):
    spark_subtype: str = "string"

    min_length: Optional[int] = None
    max_length: Optional[int] = None

    def type_specific_params(self) -> Dict[str, Any]:
        return {
            "min_length": self.min_length,
            "max_length": self.max_length,
        }


def profile_string_column(source_df: DataFrame, col_name: str) -> StringColumnProfile:
    field = source_df.schema[col_name]
    nullable = field.nullable
    null_ratio = (
        calculate_null_stats(source_df=source_df, col_name=col_name)
        if nullable
        else 0.0
    )

    length_col = F.length(F.col(col_name))

    col_profile = (
        source_df.select(length_col.alias("len")).agg(
            F.min("len").alias("min_length"),
            F.max("len").alias("max_length"),
        )
    ).first()

    col_stats = col_profile.asDict() if col_profile else {}

    return StringColumnProfile(
        col_name=col_name,
        nullable=nullable,
        null_ratio=null_ratio,
        min_length=col_stats.get("min_length"),
        max_length=col_stats.get("max_length"),
    )
