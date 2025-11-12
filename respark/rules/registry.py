from typing import Any, Dict, Type
from .rule_types import GenerationRule


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
