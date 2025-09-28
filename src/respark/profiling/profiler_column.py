from dataclasses import dataclass
from datetime import date
from typing import Dict, Any, Optional
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, Row, functions as F

# Parent Base Class
@dataclass(slots=True)
class BaseColumnProfile(ABC):
    name: str
    normalised_type: str
    nullable: bool

    @abstractmethod
    def default_rule(self) -> str: ...

    @abstractmethod
    def type_specific_params(self) -> Dict[str, Any]: ...

# Date Type Profiling
@dataclass(slots=True)
class DateColumnProfile(BaseColumnProfile):
    min_date: Optional[date] = None
    max_date: Optional[date] = None

    def default_rule(self) -> str:
        return "random_date"

    def type_specific_params(self) -> Dict[str, Any]:
        return {
            "min_date": self.min_date.isoformat() if self.min_date else None,
            "max_date": self.max_date.isoformat() if self.max_date else None,
        }


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

# Numeric Type Profiling
@dataclass(slots=True)
class NumericColumnProfile(BaseColumnProfile):
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean_value: Optional[float] = None

    def default_rule(self) -> str:
        return "random_int"

    def type_specific_params(self) -> Dict[str, Any]:
        return {
            "min_value": self.min_value,
            "max_value": self.max_value,
            "mean_value": self.mean_value
             }

def profile_numerical_column(df: DataFrame, col_name: str) -> NumericColumnProfile:
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

    return NumericColumnProfile(
        name=col_name,
        normalised_type="numeric",
        nullable=nullable,
        min_value=col_stats.get("min_value"),
        max_value=col_stats.get("max_value"),
        mean_value=col_stats.get("mean_value"),
    )

# String Type Profiling


@dataclass(slots=True)
class StringColumnProfile(BaseColumnProfile):
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    mean_length: Optional[float] = None

    def default_rule(self) -> str:
        return "random_string"

    def type_specific_params(self) -> Dict[str, Any]:
        return {
            "min_length": self.min_length,
            "max_length": self.max_length,
            "mean_length": self.mean_length
             }


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