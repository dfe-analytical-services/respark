import pprint
from typing import Any, List, Dict, Iterable, Optional
from pyspark.sql import DataFrame

from respark.sampling import UniformParentSampler
from respark.profile import (
    SchemaProfile,
    TableProfile,
    profile_schema,
    FkConstraint,
    DAG,
    CycleError,
)
from respark.plan import SchemaGenerationPlan, TableGenerationPlan, ColumnGenerationPlan
from respark.generate import SynthSchemaGenerator


class ResparkRuntime:
    """
    The runtime environment for each application of respark.
    This context manages the input source dataframes, reference dataframes,
    schema profiling, and the planning of schema generation.
    """

    def __init__(self, spark):
        self.spark = spark
        self.sources: Dict[str, DataFrame] = {}
        self.references: Dict[str, DataFrame] = {}
        self.profile: Optional[SchemaProfile] = None
        self.generation_plan: Optional[SchemaGenerationPlan] = None
        self.fk_constraints: Dict[str, FkConstraint] = {}

        self.sampler = UniformParentSampler()
        self.generated_synthetics: Dict[str, DataFrame] = {}

        # Internal attributes
        self._layers: Optional[List[List[str]]] = None

    ###
    # Profiling Methods
    ###

    def register_source(self, name: str, df: DataFrame) -> None:
        """
        Register a source DataFrame by name. Source dataframes are
        typically dataframes those that will have synthetic equivalents generated.
        """
        self.sources[name] = df

    def register_reference(self, name: str, df: DataFrame) -> None:
        """
        Register a reference DataFrame by name. Reference dataframes are
        typically lookup datasets that may be joined onto by a non-sensitive field.
        They are not typically profiled, but may be used in generation plans.
        """
        self.references[name] = df

    def profile_sources(
        self, target_sources: Optional[Iterable[str]] = None
    ) -> SchemaProfile:
        """
        Profile a subset (or all) registered sources into a SchemaProfile.
        Stores the result on self.profile and returns it.
        """

        table_map = (
            self.sources
            if target_sources is None
            else {n: self.sources[n] for n in target_sources}
        )
        self.profile = profile_schema(table_map)
        return self.profile

    def get_table_profile(self, table_name: str) -> Optional[TableProfile]:
        """
        Returns a TableProfile for a given table_name
        """
        if self.profile is None:
            return None
        return self.profile.tables.get(table_name)

    ###
    # Planning Methods
    ###

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

        self._layers = None
        return name

    def remove_fk_constraint(self, fk_name: str) -> None:
        """
        Remove by name. Raise KeyError if not found.
        """
        for name, fk in self.fk_constraints.items():
            if fk.name == fk_name:
                del self.fk_constraints[name]
                self._layers = None
                return

        raise KeyError(f"No constraint with name '{fk_name}' is currently stored")

    def list_fk_constraints(self) -> Dict[str, FkConstraint]:
        """
        Return current list of constraints.
        """
        return self.fk_constraints

    def create_generation_plan(self) -> SchemaGenerationPlan:
        """
        Using the generated schema profile,
        generate the default generation plan.
        """

        if self.profile is None:
            raise RuntimeError("Profile is not set. Call profile_sources() first.")

        table_generation_plans: List[TableGenerationPlan] = []

        for _, table_profile in self.profile.tables.items():
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

            table_generation_plans.append(
                TableGenerationPlan(
                    name=table_profile.name, row_count=row_count, columns=col_plans
                )
            )
        self.generation_plan = SchemaGenerationPlan(table_generation_plans)

        table_names = [table_plan.name for table_plan in self.generation_plan.tables]

        # missing = [
        #     (constraint.pk_table, constraint.fk_table)
        #     for constraint in self.fk_constraints
        #     if constraint.pk_table not in table_names
        #     or constraint.fk_table not in table_names
        # ]
        # if missing:
        #     raise ValueError(
        #         "Constraints reference tables outside the plan: "
        #         + ", ".join(f"{u}->{v}" for (u, v) in missing)
        #     )
        try:
            dag = DAG.from_fk_constraints(table_names, self.fk_constraints)
            self._layers = dag.compute_layers()
        except CycleError as e:
            raise RuntimeError(
                f"Cycle detected in FK relationships for current plan: {e}"
            ) from e

        return self.generation_plan

    def update_column_rule(self, table: str, col: str, rule: str) -> None:
        if self.generation_plan is None:
            raise RuntimeError("Call create_generation_plan() first.")
        self.generation_plan.update_column_rule(table, col, rule)

    def update_column_params(
        self, table: str, col: str, params: Dict[str, Any]
    ) -> None:
        if self.generation_plan is None:
            raise RuntimeError("Call create_generation_plan() first.")
        self.generation_plan.update_column_params(table, col, params)

    def update_table_row_count(self, table: str, new_row_count: int) -> None:
        if self.generation_plan is None:
            raise RuntimeError("Call create_generation_plan() first.")
        self.generation_plan.update_table_row_count(table, new_row_count)

    def get_generation_layers(self) -> List[List[str]]:
        if self.generation_plan is None:
            raise RuntimeError(
                "No generation plan. Call create_generation_plan() first."
            )
        if self._layers is None:
            names = [t.name for t in self.generation_plan.tables]
            dag = DAG.from_fk_constraints(names, self.fk_constraints)
            self._layers = dag.compute_layers()
        return self._layers

    def print_generation_layers(self) -> None:
        pprint.pprint(self._layers)

    ###
    # Generation Methods
    ###

    def generate(self, global_seed: int = 18151210) -> Dict[str, DataFrame]:
        if self.generation_plan is None:
            raise RuntimeError(
                "generation_plan is not set. Call create_generation_plan() first."
            )

        gen = SynthSchemaGenerator(
            self.spark, runtime=self, references=self.references, seed=global_seed
        )
        return gen.generate_synthetic_schema(
            schema_gen_plan=self.generation_plan,
            fk_constraints=self.fk_constraints,
        )
