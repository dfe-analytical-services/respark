import string
from abc import ABC, abstractmethod
from datetime import datetime
from pyspark.sql import Column, functions as F, types as T
from .random_helpers import RNG


class GenerationRule(ABC):
    def __init__(self, **params):
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


GENERATION_RULES_REGISTRY = {}


def register_rule(rule_name: str):
    """
    Decorator to register a generation rule class
    """

    def wrapper(cls):
        GENERATION_RULES_REGISTRY[rule_name] = cls
        return cls

    return wrapper


def get_rule(rule_name: str, **params) -> GenerationRule:
    """
    Factory to instantiate a rule by name
    """
    if rule_name not in GENERATION_RULES_REGISTRY:
        raise ValueError(f"Rule {rule_name} is not registered")
    return GENERATION_RULES_REGISTRY[rule_name](**params)


# Date Rules
@register_rule("random_date")
class RandomDateRule(GenerationRule):
    def generate_column(self) -> Column:
        min_date_str = self.params.get("min_date", "2000-01-01")
        max_date_str = self.params.get("max_date", "2025-12-31")

        min_date = datetime.strptime(min_date_str, "%Y-%m-%d")
        max_date = datetime.strptime(max_date_str, "%Y-%m-%d")
        days_range = (max_date - min_date).days

        rng = self.rng()
        offset = rng.randint(0, days_range)
        return F.date_add(F.to_date(F.lit(min_date_str)), offset).cast(T.DateType())


# Numeric Rules
@register_rule("random_int")
class RandomIntRule(GenerationRule):
    def generate_column(self) -> Column:
        min_value = self.params.get("min_value", 0)
        max_value = self.params.get("max_value", 2147483647)

        rng = self.rng()
        col = rng.randint(min_value, max_value)
        return col.cast(T.IntegerType())


# String Rules
@register_rule("random_string")
class RandomStringRule(GenerationRule):
    def generate_column(self) -> Column:
        min_length = self.params.get("min_length", 0)
        max_length = self.params.get("max_length", 50)
        charset = self.params.get("charset", string.ascii_letters)

        rng = self.rng()

        length = rng.randint(min_length, max_length, "len")
        charset_arr = F.array([F.lit(c) for c in charset])

        pos_seq = F.sequence(F.lit(0), F.lit(max_length - 1))
        chars = F.transform(pos_seq, lambda p: rng.choice(charset_arr, "pos", p))

        return F.concat_ws("", F.slice(chars, 1, length)).cast(T.StringType())
