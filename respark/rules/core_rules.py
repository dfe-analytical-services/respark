from typing import Optional, TYPE_CHECKING

from pyspark.sql import DataFrame, Column
from pyspark.sql import types as T
from respark.profile import FkConstraint
from respark.rules import GenerationRule, register_generation_rule
from respark.sampling import UniformParentSampler

if TYPE_CHECKING:
    from respark.runtime import ResparkRuntime


@register_generation_rule("sample_from_reference")
class SampleFromReference(GenerationRule):
    """
    Uniformly sample values from the DISTINCT set in a named reference DataFrame.

    Expected params:
      - reference_name str      # key in runtime.references
      - column: str   # reference column to draw values from (distinct)
    """

    # Relational rule: generate_column() not used
    def generate_column(self) -> Column:
        raise NotImplementedError(
            "SampleFromReference is relational; use apply(df, runtime, target_col)."
        )

    def apply(
        self, df: DataFrame, runtime: Optional["ResparkRuntime"], target_col: str
    ) -> DataFrame:
        if runtime is None:
            raise RuntimeError(
                "SampleFromReference requires runtime (for references and distributed chooser)."
            )

        ref_name = self.params["reference_name"]
        ref_col = self.params["column"]

        if not ref_name or not ref_col:
            raise ValueError("Params 'reference_name' and 'column' are required.")

        if ref_name not in runtime.references:
            raise ValueError(f"Reference '{ref_name}' not found in runtime.references")

        sampler = UniformParentSampler()
        artifact = sampler.ensure_artifact_for_parent(
            cache_key=(ref_name, ref_col),
            parent_df=runtime.references[ref_name],
            parent_col=ref_col,
        )

        rng = self.rng()
        out_type: T.DataType = df.schema[target_col].dataType

        salt_base = f"{self.params.get('__table', 'table')}.{target_col}"
        return sampler.assign_uniform_from_artifact(
            child_df=df,
            artifact=artifact,
            rng=rng,
            out_col=target_col,
            out_type=out_type,
            salt_partition=f"{salt_base}:part",
            salt_position=f"{salt_base}:pos",
        )


@register_generation_rule("fk_from_parent")
class ForeignKeyFromParent(GenerationRule):
    """
    Populate a child FK by uniformly sampling the parent's PK values
    from the synthetic parent produced in a prior DAG layer.

    Expected params:
      - constraint: FkConstraint    # describes pk_table, pk_column, fk_table, fk_column, name
    """

    # Relational rule: generate_column() not used
    def generate_column(self) -> Column:
        raise NotImplementedError(
            "ForeignKeyFromParent is relational; use apply(df, runtime, target_col)."
        )

    def _find_fk_constraint(
        self, runtime: "ResparkRuntime", fk_table: str, fk_column: str
    ) -> "FkConstraint":
        matches = [
            c
            for c in runtime.fk_constraints.values()
            if c.fk_table == fk_table and c.fk_column == fk_column
        ]
        if not matches:
            raise ValueError(f"No FK constraint found for {fk_table}.{fk_column}")
        if len(matches) > 1:
            names = [c.name for c in matches]
            raise ValueError(
                f"Multiple FK constraints found for {fk_table}.{fk_column}: {names}. "
                f"Disambiguate (e.g., by passing a constraint_name) or consolidate constraints."
            )
        return matches[0]

    def apply(
        self, df: DataFrame, runtime: Optional["ResparkRuntime"], target_col: str
    ) -> DataFrame:
        if runtime is None:
            raise RuntimeError("ForeignKeyFromParent requires runtime.")
        fk_table = self.params.get("__table")
        if not fk_table:
            raise ValueError(
                "Missing '__table' in rule params; generator should inject it."
            )

        constraint = self._find_fk_constraint(
            runtime, fk_table=fk_table, fk_column=target_col
        )

        if constraint.fk_column != target_col:
            raise ValueError(
                f"Constraint targets {constraint.fk_table}.{constraint.fk_column} "
                f"but rule is populating {target_col}"
            )
        if constraint.pk_table not in runtime.generated_synthetics:
            raise ValueError(
                f"Synthetic parent table '{constraint.pk_table}' not present. "
                "Ensure DAG layers run parents before children."
            )
        parent_df = runtime.generated_synthetics[constraint.pk_table]

        sampler = UniformParentSampler()
        artifact = sampler.ensure_artifact_for_parent(
            cache_key=(constraint.pk_table, constraint.pk_column),
            parent_df=parent_df,
            parent_col=constraint.pk_column,
            distinct=False,
        )

        rng = self.rng()

        out_type: T.DataType = df.schema[target_col].dataType

        salt = constraint.name or f"{constraint.fk_table}.{constraint.fk_column}"
        return sampler.assign_uniform_from_artifact(
            child_df=df,
            artifact=artifact,
            rng=rng,
            out_col=target_col,
            out_type=out_type,
            salt_partition=f"{salt}:part",
            salt_position=f"{salt}:pos",
        )
