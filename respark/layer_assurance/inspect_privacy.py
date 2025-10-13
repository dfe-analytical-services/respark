from dataclasses import dataclass
from typing import Dict, List, ClassVar
from pyspark.sql import DataFrame, functions as F
from .engine_rules import register_inspection_rule, InspectionRule
from .engine_reporting import InspectionResult

# Rule-specific Parameters


@dataclass(frozen=True)
class PrivacyParams:
    source_data: Dict[str, DataFrame]
    synth_data: Dict[str, DataFrame]
    sensitive_columns: Dict[str, List[str]]


# Structural Rules
@register_inspection_rule("check_schema_structure")
class ColumnMatchesCheck(InspectionRule[PrivacyParams]):

    params: ClassVar[type[PrivacyParams]] = PrivacyParams

    def inspect(self) -> InspectionResult:
        """
        Compare synthetic dataset back to source dataset, checking for matches
        to production values
        """
        source_data = self.params.source_data
        synth_data = self.params.synth_data
        sensitive_columns = self.params.sensitive_columns

        theme = "Privacy"
        name = "check_column_level_matches"

        matched_columns: List[str] = []

        for key, source_df in source_data.items():
            print(f"insepcting table {key}")
            synth_df = synth_data[key]

            cols_to_inspect = sensitive_columns.get(key, [])
            print(cols_to_inspect)
            if not cols_to_inspect:
                print(f"no cols to inspect in {key}")
                continue

            for col_name in cols_to_inspect:

                matched_values = (
                    source_df.select(F.col(col_name))
                    .distinct()
                    .intersect(synth_df.select(F.col(col_name)).distinct())
                )

                if matched_values.count() > 0:
                    matched_columns.append(f"{key}.{col_name}")
                    status = "Failed"
                    message = f"Sensitive value match detected in {matched_columns[0]}"
                    return InspectionResult(
                        theme=theme, name=name, status=status, message=message
                    )

        status = "Passed"
        message = "OK"
        return InspectionResult(theme=theme, name=name, status=status, message=message)
