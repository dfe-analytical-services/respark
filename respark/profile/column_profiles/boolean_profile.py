from dataclasses import dataclass
from typing import TypedDict, Optional, Dict
from pyspark.sql import DataFrame, functions as F
from .base_column_profile import BaseColumnProfile, calculate_null_stats


class BooleanParams(TypedDict):
    p_true: Optional[float]


@dataclass(slots=True)
class BooleanColumnProfile(BaseColumnProfile[BooleanParams]):
    spark_subtype: str = "boolean"
    p_true: Optional[float] = None

    def type_specific_params(self) -> BooleanParams:
        return {"p_true": self.p_true}


def profile_boolean_column(source_df: DataFrame, col_name: str) -> BooleanColumnProfile:
    field = source_df.schema[col_name]
    nullable = field.nullable
    null_ratio = (
        calculate_null_stats(source_df=source_df, col_name=col_name)
        if nullable
        else 0.0
    )

    col_profile = (
        source_df.select(F.col(col_name).alias("val"))
        .agg(
            F.count(F.when(F.col("val") == True, True)).alias("true_count"),
            F.count(F.when(F.col("val") == False, True)).alias("false_count"),
        )
        .first()
    )

    col_stats: Dict[str, int] = col_profile.asDict() if col_profile else {}

    true_count = col_stats["true_count"]
    false_count = col_stats["false_count"]
    p_true = (
        float(true_count / (true_count + false_count))
        if (true_count and false_count)
        else None
    )

    return BooleanColumnProfile(
        col_name=col_name,
        nullable=nullable,
        null_ratio=null_ratio,
        p_true=p_true,
    )
