import json
from typing import Optional, Dict, Any, List, Set
from dataclasses import dataclass, field, asdict
from ..relationships import FkConstraint, InternalColDepndency, DAG, CycleError
from respark.rules.rule_types import RelationalGenerationRule
from respark.rules.registry import get_generation_rule


@dataclass
class ColumnGenerationPlan:
    name: str
    data_type: str
    rule_name: str
    params: Dict[str, Any] = field(default_factory=dict)
    parent_columns: Set[str] = field(default_factory=set)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def update_parent_columns(self):
        rule = get_generation_rule(rule_name=self.rule_name, params=self.params)
        if isinstance(rule, RelationalGenerationRule):
            self.parent_columns = rule.collect_parent_columns()


@dataclass
class TableGenerationPlan:
    name: str
    row_count: int
    column_plans: List[ColumnGenerationPlan] = field(default_factory=list)
    column_dependencies: Dict[str, InternalColDepndency] = field(default_factory=dict)
    column_generation_layers: Optional[List[List[str]]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, sort_keys=False)

    ###
    # Intra-Table Column Relationships
    ##

    def update_column_dependencies(self):

        updated_col_dependencies = {}
        for col_plan in self.column_plans:
            col_plan.update_parent_columns()
            parent_cols_set = col_plan.parent_columns

            if parent_cols_set:
                for parent_col in parent_cols_set:
                    name = InternalColDepndency.derive_name(parent_col, self.name)
                    updated_col_dependencies[name] = InternalColDepndency(
                        parent_col=parent_col,
                        child_col=self.name,
                    )
        self.column_dependencies = updated_col_dependencies
        self.column_generation_layers = None

    def get_column_dependencies(self) -> Dict[str, InternalColDepndency]:
        """
        Return current dict of constraints.
        """
        return self.column_dependencies

    def build_inter_col_dependencies(self) -> None:

        self.update_column_dependencies()

        try:
            col_names = {plan.name for plan in self.column_plans}
            col_dependencies = (
                {"start_node": dep.parent_col, "end_node": dep.child_col}
                for dep in self.column_dependencies.values()
            )
            col_dag = DAG.build(col_names, col_dependencies)
            self.column_generation_layers = col_dag.compute_layers()

        except CycleError as e:
            raise RuntimeError(
                f"Cycle detected in inter-column dependencies for current plan: {e}"
            ) from e


@dataclass
class SchemaGenerationPlan:
    table_plans: List[TableGenerationPlan] = field(default_factory=list)
    fk_constraints: Dict[str, FkConstraint] = field(default_factory=dict)
    table_generation_layers: Optional[List[List[str]]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, sort_keys=False)

    ###
    # Inter-Table FK Relationships
    ##

    def add_fk_constraint(
        self, pk_table: str, pk_col: str, fk_table: str, fk_col: str
    ) -> str:
        """
        Add a new FK constraint. Returns the generated name.
        Raises ValueError if a constraint with the same name already exists.
        """

        name = FkConstraint.derive_name(pk_table, pk_col, fk_table, fk_col)

        if name in self.fk_constraints:
            raise ValueError(f"Constraint '{name}' already present")

        self.fk_constraints[name] = FkConstraint(
            pk_table=pk_table,
            pk_column=pk_col,
            fk_table=fk_table,
            fk_column=fk_col,
        )

        self.table_generation_layers = None
        return name

    def remove_fk_constraint(self, fk_name: str) -> None:
        """
        Remove by name. Raise KeyError if not found.
        """
        for name, fk in self.fk_constraints.items():
            if fk.name == fk_name:
                del self.fk_constraints[name]
                self.table_generation_layers = None
                return

        raise KeyError(f"No constraint with name '{fk_name}' is currently stored")

    def list_fk_constraints(self) -> Dict[str, FkConstraint]:
        """
        Return current list of constraints.
        """
        return self.fk_constraints

    ###
    # Table Plan APIs
    ###

    def get_table_plan(self, table_name: str) -> TableGenerationPlan:
        for table in self.table_plans:
            if table.name == table_name:
                return table
        raise ValueError(f"Table {table_name} not found in the generation plan.")

    def update_table_row_count(self, table_name: str, new_row_count: int) -> None:
        for table in self.table_plans:
            if table.name == table_name:
                table.row_count = new_row_count
                return
        raise ValueError(f"Table {table_name} not found in the generation plan.")

    def build_inter_table_dependencies(self) -> None:
        for table_plan in self.table_plans:
            table_plan.build_inter_col_dependencies()

        try:
            table_names = {table_plan.name for table_plan in self.table_plans}
            table_dependencies = (
                {"start_node": dep.pk_table, "end_node": dep.fk_table}
                for dep in self.fk_constraints.values()
            )
            table_dag = DAG.build(table_names, table_dependencies)
            self.table_generation_layers = table_dag.compute_layers()
        except CycleError as e:
            raise RuntimeError(
                f"Cycle detected in FK relationships for current plan: {e}"
            ) from e

    ###
    # Column Plan APIs
    ###

    def get_column_plan(
        self, table_name: str, column_name: str
    ) -> ColumnGenerationPlan:
        for table in self.table_plans:
            if table.name == table_name:
                for column_plan in table.column_plans:
                    if column_plan.name == column_name:
                        return column_plan
        raise ValueError(f"Column {column_name} not found in table {table_name}.")

    def update_column_rule(
        self,
        table_name: str,
        column_name: str,
        new_rule: str,
    ) -> None:
        for table in self.table_plans:
            if table.name == table_name:
                for column_plan in table.column_plans:
                    if column_plan.name == column_name:
                        column_plan.rule_name = new_rule
                        return
        raise ValueError(f"Column {column_name} not found in table {table_name}.")

    def update_column_params(
        self,
        table_name: str,
        column_name: str,
        new_params: Dict[str, Any],
    ) -> None:
        for table in self.table_plans:
            if table.name == table_name:
                for column in table.column_plans:
                    if column.name == column_name:
                        column.params.update(new_params)
                        return
        raise ValueError(f"Column {column_name} not found in table {table_name}.")
