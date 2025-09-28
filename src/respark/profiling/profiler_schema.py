from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Union
from pyspark.sql import DataFrame

from .profiler_table import TableProfile, profile_table

  
@dataclass(slots=True)
class SchemaProfile:
    tables: Dict[str, TableProfile] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)



class SchemaProfiler:
    def __init__(self, default_single_name: str = "table") -> None:
        self._default_single_name = default_single_name

    def profile_schema(self, tables: Union[DataFrame, Dict[str, DataFrame]]) -> SchemaProfile:
        table_map = self._as_table_map(tables, self._default_single_name)

        table_profiles: Dict[str, TableProfile] = {}
        for table_name, df in table_map.items():
            table_profiles[table_name] = profile_table(df, table_name)

        return SchemaProfile(tables=table_profiles)


    @staticmethod
    def _as_table_map(tables: Union[DataFrame, Dict[str, DataFrame]], default_single_name: str) -> Dict[str, DataFrame]:
        if isinstance(tables, DataFrame):
            return {default_single_name: tables}
        if isinstance(tables, dict):
            for k, v in tables.items():
                if not isinstance(k, str) or not isinstance(v, DataFrame):
                    raise TypeError("Expected Dict[str, DataFrame]")
            return tables
        raise TypeError("Expected a DataFrame or Dict[str, DataFrame]")


   