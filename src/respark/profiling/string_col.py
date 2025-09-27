
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, functions as F

from .base_col import ColumnProfile


@dataclass(slots=True)
class StringColumnProfile(ColumnProfile):
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    mean_length: Optional[float] = None


def profile_string_column(df: DataFrame, col_name: str) -> StringColumnProfile:
    field = df.schema[col_name]
    nullable = field.nullable

    length_col = F.length(F.col(col_name))

    col_profile = (
        df
        .select(length_col.alias("len"))
        .agg(
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
        min_length=col_stats.get("min_length"),
        max_length=col_stats.get("max_length"),
        mean_length=col_stats.get("mean_length"),
    )

