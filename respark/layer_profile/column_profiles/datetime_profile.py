from dataclasses import dataclass
from datetime import date
from typing import ClassVar, TypedDict, Literal, Optional
from pyspark.sql import DataFrame, functions as F
from .base_profile import BaseColumnProfile


# Parameters unique to Date values
class DateParams(TypedDict):
    min_date: Optional[str]
    max_date: Optional[str]


# Date Column Profile Class
@dataclass(slots=True)
class DateColumnProfile(BaseColumnProfile[DateParams]):
    spark_subtype: ClassVar[Literal["date"]] = "date"
    min_date: Optional[date] = None
    max_date: Optional[date] = None

    def default_rule(self) -> str:
        return "random_date"

    def type_specific_params(self) -> DateParams:
        return {
            "min_date": self.min_date.isoformat() if self.min_date else None,
            "max_date": self.max_date.isoformat() if self.max_date else None,
        }


def profile_date_column(df: DataFrame, col_name: str) -> DateColumnProfile:

    field = df.schema[col_name]
    nullable = field.nullable

    col_profile = (
        df.select(F.col(col_name).alias("val")).agg(
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
