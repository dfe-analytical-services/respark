from dataclasses import dataclass
from typing import Optional
from pyspark.sql import DataFrame, functions as F
from .base_col import ColumnProfile

@dataclass(slots=True)
class NumericalColumnProfile(ColumnProfile):
    min_value: Optional[int]
    max_value: Optional[int]
    mean_value: Optional[float]

def profile_numerical_column(df: DataFrame, col_name: str) -> NumericalColumnProfile:
    field = df.schema[col_name]
    nullable = field.nullable

    col_profile = (
        df
        .select(F.col(col_name).alias("val"))
        .agg(
            F.min("val").alias("min_value"),
            F.max("val").alias("max_value"),
            F.avg("val").alias("mean_value"),
        )
    ).first()

    col_stats = col_profile.asDict() if col_profile else {}

    return NumericalColumnProfile(
        name=col_name,
        normalised_type="numeric",
        nullable=nullable,
        min_value=col_stats.get("min_value"),
        max_value=col_stats.get("max_value"),
        mean_value=col_stats.get("mean_value"),
    )
