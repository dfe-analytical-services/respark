from abc import ABC, abstractmethod
from typing import Any, Dict, Type
from pyspark.sql import Column
from .numeric_utils import RNG


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
        pass


GENERATION_RULES_REGISTRY: Dict[str, Type["GenerationRule"]] = {}


def register_generation_rule(rule_name: str):
    """
    Decorator to register a generation rule class
    """

    def wrapper(rule_class: Type["GenerationRule"]) -> Type["GenerationRule"]:
        GENERATION_RULES_REGISTRY[rule_name] = rule_class
        return rule_class

    return wrapper


def get_generation_rule(rule_name: str, **params: Any) -> GenerationRule:
    """
    Factory to instantiate a rule by name
    """
    try:
        rule_class: Type["GenerationRule"] = GENERATION_RULES_REGISTRY[rule_name]
        return rule_class(**params)
    except KeyError:
        raise ValueError(f"Rule {rule_name} is not registered")


@register_generation_rule("reuse_from_set")
class ReuseFromSet(GenerationRule):
    def get_set_values(self):
        set_df = self.params["reference_df"]
        set_col = self.params["reference_col"]
        values = set_df.select(set_col).distinct().rdd.map(lambda r: r[0]).collect()
        return values

    def generate_column(self) -> Column:
        values = self.get_set_values()
        rng = self.rng()
        return rng.choice(values)
