
import string
from abc import ABC, abstractmethod
from datetime import datetime
from pyspark.sql import Column, functions as F, types as T

class GenerationRule(ABC):
    def __init__(self, **params):
        self.params = params

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

def get_rule(rule_name:str, **params) -> GenerationRule:
    """
    Factory to instantiate a rule by name
    """
    if rule_name not in GENERATION_RULES_REGISTRY:
        raise ValueError(f"Rule {rule_name} is not registered")
    return GENERATION_RULES_REGISTRY[rule_name](**params)

#Date Rules
@register_rule("random_date")
class RandomDateRule(GenerationRule):
    def generate_column(self) -> Column:
        min_date_str = self.params.get("min_date", "2000-01-01")
        max_date_str = self.params.get("max_date", "2025-12-31")

        min_date = datetime.strptime(min_date_str, "%Y-%m-%d")
        max_date = datetime.strptime(max_date_str, "%Y-%m-%d")

        days_range = (max_date - min_date).days
        random_days = (F.floor(F.rand() * (days_range + 1))).cast(T.IntegerType())

        return F.date_add(F.lit(min_date_str), random_days).cast(T.DateType())

#Numeric Rules
@register_rule("random_int")
class RandomIntRule(GenerationRule):
    def generate_column(self) -> Column:
        min_value = self.params.get("min_value", 0)
        max_value = self.params.get("max_value", 2147483647)
        return (F.floor(F.rand() * (max_value- min_value + 1))+min_value).cast("int")
    
#String Rules
@register_rule("random_string")
class RandomStringRule(GenerationRule):
    def generate_column(self) -> Column:
        min_length = self.params.get("min_length", 0)
        max_length = self.params.get("max_length", 50)
        charset = self.params.get(
            "charset",
            string.ascii_letters)

        random_length = (
            F.floor(F.rand() * (max_length - min_length + 1)) + min_length
        ).cast("int")

        exprs = [
            F.element_at(
                F.array([F.lit(c) for c in charset]),
                (F.floor(F.rand() * len(charset)) + 1).cast("int")
            ) for _ in range(max_length)
        ]

        return F.concat_ws("", F.slice(F.array(*exprs), 1, random_length))
