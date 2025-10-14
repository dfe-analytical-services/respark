from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Union
from pyspark.sql import DataFrame
from .table_profile import TableProfile, profile_table


@dataclass(slots=True)
class SchemaProfile:
    tables: Dict[str, TableProfile] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def profile_schema(
    tables: Union[DataFrame, Dict[str, DataFrame]], default_single_name: str = "table"
) -> SchemaProfile:
    table_map = _as_table_map(tables, default_single_name)
    table_profiles: Dict[str, TableProfile] = {
        name: profile_table(df, name) for name, df in table_map.items()
    }
    return SchemaProfile(tables=table_profiles)


def _as_table_map(
    tables: Union[DataFrame, Dict[str, DataFrame]], default_single_name: str
) -> Dict[str, DataFrame]:
    if isinstance(tables, DataFrame):
        return {default_single_name: tables}
    if isinstance(tables, dict):
        for k, v in tables.items():
            if not isinstance(k, str) or not isinstance(v, DataFrame):
                raise TypeError("Expected Dict[str, DataFrame]")
        return tables
    raise TypeError("Expected a DataFrame or Dict[str, DataFrame]")
