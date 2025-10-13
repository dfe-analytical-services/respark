from dataclasses import dataclass
from typing import TypedDict, Literal, Optional
from pyspark.sql import DataFrame, functions as F, types as T
from .base_profile import BaseColumnProfile


# Parameters unique to Boolean columns
class BooleanParams(TypedDict):
    percentage_true: Optional[float]
    percentage_false: Optional[float]
    percentage_null: Optional[float]


# Date Column Profile Class
@dataclass(slots=True)
class BooleanColumnProfile(BaseColumnProfile[BooleanParams]):
    percentage_true: Optional[float] = None
    percentage_false: Optional[float] = None
    percentage_null: Optional[float] = None

    spark_subtype: Literal["boolean"] = "boolean"

    def default_rule(self) -> str:
        return "random_boolean"

    def type_specific_params(self) -> BooleanParams:
        return {
            "percentage_true": self.percentage_true,
            "percentage_false": self.percentage_false,
            "percentage_null": self.percentage_null,
        }


def profile_boolean_column(df: DataFrame, col_name: str) -> BooleanColumnProfile:
    field = df.schema[col_name]
    nullable = field.nullable
    spark_subtype = "boolean"

    col_profile = (
        df.select(F.col(col_name).alias("val"))
        .agg(
            F.count(F.when(F.col("val") == True, True)).alias("true_count"),
            F.count(F.when(F.col("val") == False, True)).alias("false_count"),
            F.count(F.when(F.col("val").isNull(), True)).alias("null_count"),
            F.count("*").alias("total_count"),
        )
        .first()
    )

    col_stats = col_profile.asDict() if col_profile else {}

    total = col_stats["total_count"]
    percentage_true = round(col_stats["true_count"] / total, 2) if total else None
    percentage_false = round(col_stats["false_count"] / total, 2) if total else None
    percentage_null = round(col_stats["null_count"] / total, 2) if total else None

    return BooleanColumnProfile(
        name=col_name,
        normalised_type="boolean",
        nullable=nullable,
        spark_subtype=spark_subtype,
        percentage_true=percentage_true,
        percentage_false=percentage_false,
        percentage_null=percentage_null,
    )
