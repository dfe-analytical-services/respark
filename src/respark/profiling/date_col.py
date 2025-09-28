from datetime import date
from dataclasses import dataclass
from typing import Optional
from pyspark.sql import DataFrame, functions as F
from .base_col import ColumnProfile

@dataclass(slots=True)
class DateColumnProfile(ColumnProfile):
    min_date: Optional[date]
    max_date: Optional[date]

def profile_date_column(df: DataFrame, col_name: str) -> DateColumnProfile:
    field = df.schema[col_name]
    nullable = field.nullable

    col_profile = (
        df
        .select(F.col(col_name).alias("val"))
        .agg(
            F.min("val").alias("min_date"),
            F.max("val").alias("max_date"),
        )
    ).first()

    col_stats = col_profile.asDict() if col_profile else {}

    return DateColumnProfile(
        name=col_name,
        normalised_type="date",
        nullable=nullable,
        min_date=col_stats.get("min_date"),
        max_date=col_stats.get("max_date"),
    )