from dataclasses import dataclass
from typing import ClassVar, TypedDict, Literal, Optional
from pyspark.sql import DataFrame, functions as F, types as T
from .base_column_profile import BaseColumnProfile, calculate_null_stats


class DateTimeParams(TypedDict):
    min_iso: Optional[str]
    max_iso: Optional[str]
    precision: Optional[int]
    min_epoch_micros: Optional[int]
    max_epoch_micros: Optional[int]


@dataclass(slots=True)
class DateTimeColumnProfile(BaseColumnProfile[DateTimeParams]):
    spark_subtype: ClassVar[Literal["date", "timestamp_ltz", "timestamp_ntz"]]
    min_iso: Optional[str] = None
    max_iso: Optional[str] = None
    precision: Optional[int] = None
    min_epoch_micros: Optional[int] = None
    max_epoch_micros: Optional[int] = None

    def type_specific_params(self) -> DateTimeParams:
        return {
            "min_iso": self.min_iso,
            "max_iso": self.max_iso,
            "precision": self.precision,
            "min_epoch_micros": self.min_epoch_micros,
            "max_epoch_micros": self.max_epoch_micros,
        }


# Date Column Profile Class
@dataclass(slots=True)
class DateColumnProfile(DateTimeColumnProfile):
    spark_subtype = "date"


@dataclass(slots=True)
class TimestampColumnProfile(DateTimeColumnProfile):
    spark_subtype = "timestamp_ltz"


@dataclass(slots=True)
class TimestampNTZColumnProfile(DateTimeColumnProfile):
    spark_subtype = "timestamp_ntz"


def profile_datetime_column(
    source_df: DataFrame, col_name: str
) -> DateTimeColumnProfile:
    field = source_df.schema[col_name]
    nullable = field.nullable
    null_ratio = (
        calculate_null_stats(source_df=source_df, col_name=col_name)
        if nullable
        else 0.0
    )
    data_type = field.dataType
    precision: Optional[int] = None
    min_epoch_micros: Optional[int] = None
    max_epoch_micros: Optional[int] = None

    if isinstance(data_type, T.DateType):
        DateTimeClass = DateColumnProfile
    elif isinstance(data_type, T.TimestampType):
        DateTimeClass = TimestampColumnProfile
    elif isinstance(data_type, T.TimestampNTZType):
        DateTimeClass = TimestampNTZColumnProfile
    else:
        raise TypeError(f"Column {col_name} is not a datetime type: {data_type}")

    col_profile = source_df.select(
        F.min(F.col(col_name)).alias("min_ts"), F.max(F.col(col_name)).alias("max_ts")
    )

    if isinstance(data_type, T.DateType):
        formatted_profile = col_profile.select(
            F.date_format("min_ts", "yyyy-MM-dd").alias("min_iso"),
            F.date_format("max_ts", "yyyy-MM-dd").alias("max_iso"),
        )
    else:
        formatted_profile = col_profile.select(
            F.date_format("min_ts", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS").alias("min_iso"),
            F.date_format("max_ts", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS").alias("max_iso"),
        )

    col_stats = formatted_profile.first()
    min_iso = col_stats["min_iso"] if col_stats and col_stats["min_iso"] else None
    max_iso = col_stats["max_iso"] if col_stats and col_stats["max_iso"] else None

    if isinstance(data_type, (T.TimestampType, T.TimestampNTZType)):
        precision_row = source_df.select(
            F.max(
                F.length(
                    F.regexp_extract(
                        F.date_format(F.col(col_name), "yyyy-MM-dd HH:mm:ss.SSSSSS"),
                        r"\.(\d+)$",
                        1,
                    )
                )
            ).alias("precision")
        ).first()
        if precision_row and precision_row["precision"] is not None:
            try:
                precision = int(precision_row["precision"])
            except Exception:
                precision = None

        if isinstance(data_type, T.TimestampType):
            us_row = source_df.select(
                F.unix_micros(F.min(F.col(col_name))).alias("min_us"),
                F.unix_micros(F.max(F.col(col_name))).alias("max_us"),
            ).first()
            min_epoch_micros = (
                int(us_row["min_us"])
                if us_row and us_row["min_us"] is not None
                else None
            )
            max_epoch_micros = (
                int(us_row["max_us"])
                if us_row and us_row["max_us"] is not None
                else None
            )

    return DateTimeClass(
        col_name=col_name,
        nullable=nullable,
        null_ratio=null_ratio,
        min_iso=min_iso,
        max_iso=max_iso,
        precision=precision,
        min_epoch_micros=min_epoch_micros,
        max_epoch_micros=max_epoch_micros,
    )
