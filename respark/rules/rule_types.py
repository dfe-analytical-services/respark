from abc import ABC, abstractmethod
from typing import Any, Optional, TYPE_CHECKING

from pyspark.sql import DataFrame, Column
from respark.random import RNG

if TYPE_CHECKING:
    from respark.runtime import ResparkRuntime


class GenerationRule(ABC):
    def __init__(self, **params: Any) -> None:
        self.params = params

    @property
    def seed(self) -> int:
        return int(self.params["__seed"])

    @property
    def row_idx(self) -> Column:
        return self.params["__row_idx"]

    def rng(self) -> RNG:
        return RNG(self.row_idx, self.seed)

    def generate_column(self) -> Column:
        """
        Placeholder for simple, non-relational rules that do not have a
        dependency on other columns or tables in order to generate a value.

        Child rules that are simple in nature (e.g random_int) should override
        this abstract method.
        """
        raise NotImplementedError

    def apply(
        self, base_df: DataFrame, runtime: "ResparkRuntime", target_col: str
    ) -> DataFrame:
        """
        Default behavior for non-relational rules: attach a Column built by generate_column().
        """
        return base_df.withColumn(target_col, self.generate_column())

    def collect_parent_columns(self) -> set:
        """
        Placeholder for complex rules that have complex dependencies (e.g fk_from_parent).

        Allows for the rules to register dependencies pre-generation to the runtime,
        to enable layering and ordering of table and column generation.
        """
        return set()


class RelationalGenerationRule(GenerationRule, ABC):
    @abstractmethod
    def collect_parent_columns(self) -> set:
        """
        Placeholder for complex rules that have complex dependencies (e.g fk_from_parent).

        Allows for the rules to register dependencies pre-generation to the runtime,
        to enable layering and ordering of table and column generation.
        """
        ...
