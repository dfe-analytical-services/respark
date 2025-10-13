import json
from typing import Dict, Any, List
from dataclasses import dataclass, field, asdict
from respark.layer_profile import SchemaProfile


@dataclass
class ColumnGenerationPlan:
    name: str
    data_type: str
    rule: str
    params: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, sort_keys=False)



@dataclass
class TableGenerationPlan:
    name: str
    row_count: int
    columns: List[ColumnGenerationPlan] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, sort_keys=False)


@dataclass
class SchemaGenerationPlan:
    tables: List[TableGenerationPlan] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, sort_keys=False)

    def get_table_plan(self, table_name: str) -> TableGenerationPlan:
        for table in self.tables:
            if table.name == table_name:
                return table
        raise ValueError(f"Table {table_name} not found in the generation plan.")

    def get_column_plan(
        self, table_name: str, column_name: str
    ) -> ColumnGenerationPlan:
        for table in self.tables:
            if table.name == table_name:
                for column in table.columns:
                    if column.name == column_name:
                        return column
        raise ValueError(f"Column {column_name} not found in table {table_name}.")

    def update_column_rule(
        self,
        table_name: str,
        column_name: str,
        new_rule: str,
    ) -> None:
        for table in self.tables:
            if table.name == table_name:
                for column_plan in table.columns:
                    if column_plan.name == column_name:
                        column_plan.rule = new_rule
                        return
        raise ValueError(f"Column {column_name} not found in table {table_name}.")

    def update_column_params(
        self,
        table_name: str,
        column_name: str,
        new_params: Dict[str, Any],
    ) -> None:
        for table in self.tables:
            if table.name == table_name:
                for column in table.columns:
                    if column.name == column_name:
                        column.params.update(new_params)
                        return
        raise ValueError(f"Column {column_name} not found in table {table_name}.")

    def update_table_row_count(self, table_name: str, new_row_count: int) -> None:
        for table in self.tables:
            if table.name == table_name:
                table.row_count = new_row_count
                return
        raise ValueError(f"Table {table_name} not found in the generation plan.")


def create_generation_plan(schema_profile: SchemaProfile) -> SchemaGenerationPlan:
    tables: List[TableGenerationPlan] = []

    for _, table_profile in schema_profile.tables.items():
        col_plans: List[ColumnGenerationPlan] = []
        row_count = table_profile.row_count

        for _, column_profile in table_profile.columns.items():
            col_plans.append(
                ColumnGenerationPlan(
                    name=column_profile.name,
                    data_type=column_profile.spark_subtype,
                    rule=column_profile.default_rule(),
                    params=column_profile.type_specific_params(),
                )
            )

        tables.append(
            TableGenerationPlan(
                name=table_profile.name, row_count=row_count, columns=col_plans
            )
        )

    return SchemaGenerationPlan(tables)
