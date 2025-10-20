from typing import Optional, TYPE_CHECKING

from pyspark.sql import DataFrame, Column
from pyspark.sql import types as T
from respark.profile import FkConstraint
from respark.rules import GenerationRule, register_generation_rule
from respark.sampling import UniformParentSampler

if TYPE_CHECKING:
    from respark.runtime import ResparkRuntime


@register_generation_rule("reuse_from_set")
class ReuseFromSet(GenerationRule):
    """
    Uniformly choose values for a column from the DISTINCT set in a named reference DataFrame.

    Expected params:
      - ref_name: str        # key in runtime.references
      - reference_col: str   # the column to draw values from (distinct)
    """

    # This rule is relational; generate_column() is not used
    def generate_column(self) -> Column:
        raise NotImplementedError(
            "ReuseFromSet is relational; use apply(df, runtime, target_col)."
        )

    def apply(
        self, df: DataFrame, runtime: Optional["ResparkRuntime"], target_col: str
    ) -> DataFrame:
        if runtime is None:
            raise RuntimeError(
                "ReuseFromSet requires runtime (for references and distributed chooser)."
            )

        ref_name = self.params["ref_name"]
        ref_col: str = self.params["reference_col"]

        if ref_name not in runtime.references:
            raise ValueError(f"Reference '{ref_name}' not found in runtime.references")

        sampler = UniformParentSampler()
        # Build or reuse artifacts for this distinct set
        artifact = sampler.ensure_artifact_for_parent(
            cache_key=(ref_name, ref_col),
            parent_df=runtime.references[ref_name],
            parent_col=ref_col,
        )

        rng = self.rng()
        out_type: T.DataType = df.schema[target_col].dataType

        # Use independent salts for partition vs position choices (deterministic per-row)
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


@register_generation_rule("fk_from_constraint")
class FkFromConstraint(GenerationRule):
    """
    Assign fk_table.fk_column by uniformly sampling pk_table.pk_column
    from the synthetic parent produced in a prior DAG layer.
    """

    # This rule is relational; generate_column() is not used
    def generate_column(self) -> Column:
        raise NotImplementedError(
            "FkFromConstraint is relational; use apply(df, runtime, target_col)."
        )

    def apply(
        self, df: DataFrame, runtime: Optional["ResparkRuntime"], target_col: str
    ) -> DataFrame:
        if runtime is None:
            raise RuntimeError(
                "FkFromConstraint requires runtime (synthetics and distributed chooser)."
            )

        c: FkConstraint = self.params["constraint"]

        if c.fk_column != target_col:
            raise ValueError(
                f"Constraint targets {c.fk_table}.{c.fk_column} but rule is populating {target_col}"
            )

        if c.pk_table not in runtime.synthetics:
            raise ValueError(
                f"Synthetic parent table '{c.pk_table}' not present. "
                "Ensure DAG layers run parents before children."
            )
        parent_df = runtime.synthetics[c.pk_table]

        sampler = UniformParentSampler()
        artifact = sampler.ensure_artifact_for_parent(
            cache_key=(c.pk_table, c.pk_column),
            parent_df=parent_df,
            parent_col=c.pk_column,
            distinct=False,  # parent PK should already be unique
        )

        rng = self.rng()
        out_type: T.DataType = df.schema[target_col].dataType

        return sampler.assign_uniform_from_artifact(
            child_df=df,
            artifact=artifact,
            rng=rng,
            out_col=target_col,
            out_type=out_type,
            salt_partition=f"{c.name}:part",
            salt_position=f"{c.name}:pos",
        )
