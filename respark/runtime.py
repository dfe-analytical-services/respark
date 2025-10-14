from typing import Any, List, Dict, Iterable, Optional
from pyspark.sql import DataFrame, functions as F
from respark.profile import SchemaProfile, TableProfile, profile_schema
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

    def deep_profile_column(
        self,
        source_name: str,
        column_name: str,
        register_as: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> DataFrame:
        """
        Deep profile a column to inspect for distinct values and their count.
        Warning: This will reveal and persist actual values.
        """
        df = self.sources[source_name]
        deep_profile = df.groupBy(column_name).count().orderBy(F.desc("count"))
        if limit is not None:
            deep_profile = deep_profile.limit(int(limit))
        if register_as:
            self.register_reference(register_as, deep_profile)
        return deep_profile

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
        Using the generated schema profile, generate the default
        generation plan.
        """

        table_generation_plans: List[TableGenerationPlan] = []
        if self.profile is None:
            raise RuntimeError("Profile is not set. Call profile_sources() first.")
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

    ###
    # Generation Methods
    ###

    def generate(self) -> Dict[str, DataFrame]:
        if self.generation_plan is None:
            raise RuntimeError(
                "generation_plan is not set. Call create_generation_plan() first."
            )
        gen = SynthSchemaGenerator(self.spark, references=self.references)
        return gen.generate_synthetic_schema(self.generation_plan)
