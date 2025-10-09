from dataclasses import dataclass
from typing import Dict, ClassVar
from pyspark.sql import DataFrame
from .engine_rules import register_inspection_rule, InspectionRule
from .engine_reporting import InspectionResult

# Rule-specific Parameters


@dataclass(frozen=True)
class SchemaStructureParams:
    source_data: Dict[str, DataFrame]
    synth_data: Dict[str, DataFrame]


# Structural Rules
@register_inspection_rule("check_schema_structure")
class SchemaStructureCheck(InspectionRule[SchemaStructureParams]):

    params: ClassVar[type[SchemaStructureParams]] = SchemaStructureParams

    def inspect(self) -> InspectionResult:
        """
        Inspect synthetic dataset for schema structure, comparing back to
        original source dataset.
        """
        source_data = self.params.source_data
        synth_data = self.params.synth_data

        theme = "Structure"
        name = "check_schema_structure"
        status: str = ""
        message: str = ""

        if set(source_data) != set(synth_data):
            message = "Different tables between source and synthetic data"
            status = "Failed"

            return InspectionResult(
                theme=theme, name=name, status=status, message=message
            )

        else:
            message = "OK"
            status = "Passed"

            return InspectionResult(
                theme=theme, name=name, status=status, message=message
            )
