from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Union, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import types as T


@dataclass(slots=True)
class ColumnInfo:
    name: str
    normalised_type: str
    spark_type_str: str
    nullable: bool
    supported: bool


@dataclass(slots=True)
class TableInfo:
    name: str
    columns: Dict[str, ColumnInfo] = field(default_factory=dict)


@dataclass(slots=True)
class SchemaModel:
    tables: Dict[str, TableInfo] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class TypeNormaliser:
    @staticmethod
    def normalise(dt: T.DataType) -> Tuple[str, bool]:
        if isinstance(dt, T.StringType):
            return "string", True
        if isinstance(dt, (T.IntegerType, T.LongType, T.ShortType, T.ByteType)):
            return "int", True
        if isinstance(dt, (T.FloatType, T.DoubleType)):
            return "double", True
        if isinstance(dt, T.DateType):
            return "date", True
        return str(dt), False


class SchemaInspector:
    """
    Turn one or more Spark DataFrames into a SchemaModel (facts only).
    """

    def __init__(self, default_single_name: str = "table") -> None:
        self._default_single_name = default_single_name

    def inspect(self, tables: Union[DataFrame, Dict[str, DataFrame]]) -> SchemaModel:
        table_map = self._as_table_map(tables, self._default_single_name)
        model = SchemaModel()

        for table_name, df in table_map.items():
            table_info = TableInfo(name=table_name)
            for field in df.schema.fields:
                normalised_type, supported = TypeNormaliser.normalise(
                    field.dataType)
                col_info = ColumnInfo(
                    name=field.name,
                    normalised_type=normalised_type,
                    spark_type_str=str(field.dataType),
                    nullable=field.nullable,
                    supported=supported
                )
                table_info.columns[field.name] = col_info

            model.tables[table_name] = table_info

        return model

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
