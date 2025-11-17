from dataclasses import dataclass

from typing import TypeVar, Generic, ClassVar, Tuple
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, functions as F

ColTypeParams = TypeVar("ColTypeParams")


# Parent Base Class
@dataclass(slots=True)
class BaseColumnProfile(Generic[ColTypeParams], ABC):
    col_name: str
    nullable: bool
    null_ratio: float
    spark_subtype: ClassVar[str]

    def default_rule(self) -> str:
        return f"random_{self.spark_subtype}"

    @abstractmethod
    def type_specific_params(self) -> ColTypeParams: ...


def calculate_null_stats(source_df: DataFrame, col_name: str) -> float:
    if col_name not in source_df.columns:
        raise KeyError(f"Column '{col_name}' not found in DataFrame")

    count_result = (
        source_df.selectExpr(f"`{col_name}` as val")
        .agg(
            F.count(F.lit(1)).alias("total"),
            F.count(F.when(F.col("val").isNull(), 1)).alias("nulls"),
        )
        .first()
    )
    if count_result:
        total = int(count_result["total"])
        nulls = int(count_result["nulls"])
        return (nulls / total) if total else 0.0

    else:
        raise Exception(
            f"calculate_null_stats() failed to determine null_ratio from {col_name}"
        )
