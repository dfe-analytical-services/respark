from abc import ABC, abstractmethod
from typing import Any, Dict, Type, Optional, TYPE_CHECKING

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

    @abstractmethod
    def generate_column(self) -> Column:
        """
        For simple (non-relational) rules, return a per-row Column expression.
        Relational rules should override apply() and may raise NotImplementedError here.
        """
        raise NotImplementedError

    def apply(
        self, df: DataFrame, runtime: Optional["ResparkRuntime"], target_col: str
    ) -> DataFrame:
        """
        Default behavior for non-relational rules: attach a Column built by generate_column().
        Relational rules should override this to perform distributed joins.
        """
        return df.withColumn(target_col, self.generate_column())


GENERATION_RULES_REGISTRY: Dict[str, Type["GenerationRule"]] = {}


def register_generation_rule(rule_name: str):
    """
    Decorator to register a generation rule class by name.
    """

    def wrapper(rule_class: Type["GenerationRule"]) -> Type["GenerationRule"]:
        GENERATION_RULES_REGISTRY[rule_name] = rule_class
        return rule_class

    return wrapper


def get_generation_rule(rule_name: str, **params: Any) -> GenerationRule:
    """
    Factory to instantiate a rule by name.
    """
    try:
        rule_class: Type["GenerationRule"] = GENERATION_RULES_REGISTRY[rule_name]
        return rule_class(**params)
    except KeyError:
        raise ValueError(f"Rule {rule_name} is not registered")
