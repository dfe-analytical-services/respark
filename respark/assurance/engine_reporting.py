from dataclasses import dataclass, field
from typing import List, Literal

STATUS_OPTIONS = Literal["Passed", "Failed"]


@dataclass
class InspectionResult:
    theme: str
    name: str
    status: STATUS_OPTIONS
    message: str


@dataclass
class InspectionReport:
    results: List[InspectionResult] = field(default_factory=List)
