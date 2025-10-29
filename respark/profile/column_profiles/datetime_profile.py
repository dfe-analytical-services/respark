from dataclasses import dataclass
from typing import ClassVar, TypedDict, Literal, Optional
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from .base_column_profile import BaseColumnProfile


class DateTimeParams(TypedDict):
    min_iso: Optional[str]
    max_iso: Optional[str]
    frac_precision: Optional[int]
    session_time_zone: Optional[str]
    spark_timestamp_alias: Optional[str]
    min_epoch_micros: Optional[int]
    max_epoch_micros: Optional[int]


@dataclass(slots=True)
class DateTimeColumnProfile(BaseColumnProfile[DateTimeParams]):
    spark_subtype: ClassVar[Literal["date", "timestamp_ltz", "timestamp_ntz"]]

    min_iso: Optional[str] = None
    max_iso: Optional[str] = None
    frac_precision: Optional[int] = None
    session_time_zone: Optional[str] = None
    spark_timestamp_alias: Optional[str] = None
    min_epoch_micros: Optional[int] = None
    max_epoch_micros: Optional[int] = None

    def default_rule(self) -> str:
        return f"random_{self.spark_subtype}"

    def type_specific_params(self) -> DateTimeParams:
        return {
            "min_iso": self.min_iso,
            "max_iso": self.max_iso,
            "frac_precision": self.frac_precision,
            "session_time_zone": self.session_time_zone,
            "spark_timestamp_alias": self.spark_timestamp_alias,
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


def profile_datetime_column(df: DataFrame, col_name: str) -> DateTimeColumnProfile:

    field = df.schema[col_name]
    nullable = field.nullable
    data_type = field.dataType
    frac_precision: Optional[int] = None
    session_time_zone: Optional[str] = None
    spark_timestamp_alias: Optional[str] = None
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

    col_profile = df.select(
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
        frac_row = df.select(
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
        if frac_row and frac_row["precision"] is not None:
            try:
                frac_precision = int(frac_row["precision"])
            except Exception:
                frac_precision = None

        sess: SparkSession = df.sparkSession
        session_time_zone = sess.conf.get("spark.sql.session.timeZone", None)
        spark_timestamp_alias = sess.conf.get("spark.sql.timestampType", None)

        if isinstance(data_type, T.TimestampType):
            us_row = df.select(
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
        name=col_name,
        normalised_type="datetime",
        nullable=nullable,
        min_iso=min_iso,
        max_iso=max_iso,
        frac_precision=frac_precision,
        session_time_zone=session_time_zone,
        spark_timestamp_alias=spark_timestamp_alias,
        min_epoch_micros=min_epoch_micros,
        max_epoch_micros=max_epoch_micros,
    )
