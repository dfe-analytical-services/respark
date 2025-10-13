from abc import ABC, abstractmethod
from typing import Any, Dict, Type, Mapping, TypeVar, Generic, ClassVar
from .engine_reporting import InspectionResult

ParamsType = TypeVar("ParamsType")


class InspectionRule(Generic[ParamsType], ABC):
    """ "
    This is the base rule for all inspection rules.

    All subclasses (specific inspection rules) to set:
        PARAMS_MODEL: The rule-specific params dataclass type
    """

    PARAMS_TYPE: ClassVar[type]

    def __init__(self, params: ParamsType):
        self.params: ParamsType = params

    @abstractmethod
    def inspect(self) -> InspectionResult: ...


INSPECTION_RULES_REGISTRY: Dict[str, Type[InspectionRule[Any]]] = {}


def register_inspection_rule(rule_name: str):
    """
    Decorator to register an inspection rule class
    """

    def wrapper(rule_class):
        INSPECTION_RULES_REGISTRY[rule_name] = rule_class
        return rule_class

    return wrapper


def get_inspection_rule(
    rule_name: str, params_dict: Mapping[str, Any]
) -> InspectionRule:
    """
    Factory to instantiate a rule by name
    """
    if rule_name not in INSPECTION_RULES_REGISTRY:
        raise ValueError(f"Rule {rule_name} is not registered")

    rule_class = INSPECTION_RULES_REGISTRY[rule_name]
    rule_params_type: Type[Any] = rule_class.PARAMS_TYPE
    rule_params = rule_params_type(**params_dict)
    return rule_class(rule_params)
