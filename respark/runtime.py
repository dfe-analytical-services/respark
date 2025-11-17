from typing import Any, List, Dict, Iterable, Optional
from pyspark.sql import DataFrame

from respark.rules.registry import get_generation_rule
from respark.sampling import UniformParentSampler
from respark.profile import (
    SchemaProfile,
    TableProfile,
    profile_schema,
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

        self.sampler = UniformParentSampler()
        self.generated_synthetics: Dict[str, DataFrame] = {}

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

    def profile_sources(self) -> SchemaProfile:
        """
        Profile a subset (or all) registered sources into a SchemaProfile.
        Stores the result on self.profile and returns it.
        """

        self.profile = profile_schema(self.sources)
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

    def create_generation_plan(self) -> SchemaGenerationPlan:
        """
        Using the generated schema profile,
        generate the default generation plan.
        """

        if self.profile is None:
            raise RuntimeError("Profile is not set. Call profile_sources() first.")

        table_generation_plans: Dict[str, TableGenerationPlan] = {}

        for _, table_profile in self.profile.tables.items():
            col_plans: Dict[str, ColumnGenerationPlan] = {}
            row_count = table_profile.row_count

            for _, column_profile in table_profile.columns.items():
                rule_name = column_profile.default_rule()
                rule_params = column_profile.type_specific_params()
                col_plans[column_profile.col_name] = ColumnGenerationPlan(
                    col_name=column_profile.col_name,
                    data_type=column_profile.spark_subtype,
                    rule=get_generation_rule(rule_name, **rule_params),
                )

            table_generation_plans[table_profile.name] = TableGenerationPlan(
                name=table_profile.name, row_count=row_count, column_plans=col_plans
            )

        self.generation_plan = SchemaGenerationPlan(table_plans=table_generation_plans)
        return self.generation_plan

    def update_generation_plan(self) -> None:
        if self.generation_plan is None:
            raise RuntimeError("Call create_generation_plan() first.")
        self.generation_plan.build_inter_table_dependencies()

    def get_generation_layers(self) -> Optional[List[List[str]]]:
        if self.generation_plan:
            return self.generation_plan.table_generation_layers
        else:
            raise RuntimeError(
                "No generation plan. Call create_generation_plan() first."
            )

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
        )
