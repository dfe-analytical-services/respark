from dataclasses import dataclass

from typing import TypeVar, Generic
from abc import ABC, abstractmethod

ColTypeParams = TypeVar("ColTypeParams")


# Parent Base Class
@dataclass(slots=True)
class BaseColumnProfile(Generic[ColTypeParams], ABC):
    name: str
    normalised_type: str
    nullable: bool

    @abstractmethod
    def default_rule(self) -> str: ...

    @abstractmethod
    def type_specific_params(self) -> ColTypeParams: ...
