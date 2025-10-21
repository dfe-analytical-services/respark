from typing import Dict, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F, types as T
from respark.sampling import ParentIndexArtifact, build_parent_index
from respark.random import RNG


class UniformParentSampler:
    """Deterministic, distributed uniform sampler of parent values.

    This sampler assigns each row in a child DataFrame a uniformly sampled
    parent value using the distributed index built by `build_parent_index`.
    Sampling is performed without collecting parent values to the driver and
    is deterministic given a stable RNG and a stable parent layout.

    Typical usage:
        >>> sampler = UniformParentSampler()
        >>> art = sampler.ensure_artifact_for_parent(
        ...     cache_key=("customers", "customer_id"),
        ...     parent_df=parents_df,
        ...     parent_col="customer_id",
        ... )
        >>> out = sampler.assign_uniform_from_artifact(
        ...     child_df=orders_df,
        ...     artifact=art,
        ...     rng=rng,
        ...     out_col="customer_id",
        ...     out_type=T.LongType(),
        ... )

    """

    def __init__(self):
        self._artifact_cache: Dict[Tuple[str, str], ParentIndexArtifact] = {}

    def ensure_artifact_for_parent(
        self,
        cache_key: Tuple[str, str],
        parent_df: DataFrame,
        parent_col: str,
        distinct: bool = False,
    ) -> ParentIndexArtifact:
        """Build or fetch a `ParentIndexArtifact` for a parent value set.

        Args:
            cache_key: Logical identity for caching (e.g., ("customers", "id")).
            parent_df: DataFrame that contains the parent values.
            parent_col: Column name within `parent_df` to sample from.
            distinct: If True, apply `DISTINCT` to `parent_col` prior to indexing.

        Returns:
            ParentIndexArtifact: The distributed parent index and partition CDF.
        """
        art = self._artifact_cache.get(cache_key)
        if art is None:
            df = parent_df.select(parent_col)
            if distinct:
                df = df.distinct()
            art = build_parent_index(df, parent_col=parent_col)
            self._artifact_cache[cache_key] = art
        return art

    def assign_uniform_from_artifact(
        self,
        child_df: DataFrame,
        artifact: ParentIndexArtifact,
        rng: RNG,
        out_col: str,
        out_type: T.DataType,
        salt_partition: str = "part",  # independent substream salts
        salt_position: str = "pos",
    ) -> DataFrame:
        """Assign `child_df[out_col]` by uniformly sampling from parent values.

        Sampling steps:
          1) Draw `u1 ∈ [0,1)` and range-join to the broadcast CDF to select
             a partition with probability proportional to its size.
          2) Draw `u2 ∈ [0,1)` to choose a local position uniformly in
             `[1 .. __pcount__]` within that partition.
          3) Join to the parent index on `(partition, position)` to fetch `__pk__`.

        Args:
            child_df: DataFrame whose rows will receive sampled parent values.
            artifact: A `ParentIndexArtifact` produced by `build_parent_index`.
            rng: An RNG object providing `uniform_01_double(salt: str) -> Column`
                 that yields deterministic doubles in [0,1) per child row.
            out_col: Name of the output column to create on `child_df`.
            out_type: Spark SQL data type for the output column.
            salt_partition: Salt for the partition-selection RNG stream
                            (kept independent from position-selection).
            salt_position: Salt for the position-selection RNG stream.

        Returns:
            DataFrame: `child_df` with an added column `out_col` containing a
            uniformly sampled parent value (cast to `out_type`). If the parent
            artifact is empty (`artifact.total == 0`), the column is NULL.

        """
        if artifact.total == 0:
            # No parent values to sample from, so return a column of NULLs
            return child_df.withColumn(out_col, F.lit(None).cast(out_type))

        # 1) Choose partition via range-join to the CDF
        u1 = rng.uniform_01_double(salt_partition)
        ch = child_df.withColumn("__u__", u1)
        ch = ch.join(
            F.broadcast(artifact.part_cdf),
            (F.col("__u__") >= F.col("low")) & (F.col("__u__") < F.col("high")),
            "inner",
        ).withColumnRenamed("__ppart__", "__ppart_c__")

        # 2) Choose a local position within the selected partition [1..__pcount__]
        u2 = rng.uniform_01_double(salt_position)
        ch = ch.withColumn(
            "__tpos__", (F.floor(u2 * F.col("__pcount__")) + F.lit(1)).cast("int")
        )

        # 3) Join to parent index on (partition, position) to fetch __pk__
        par = artifact.parents_idx.select(
            "__ppart__", "__ppos__", F.col("__pk__").alias("__chosen__")
        )

        out = (
            ch.alias("ch")
            .join(
                par.alias("par"),
                (F.col("ch.__ppart_c__") == F.col("par.__ppart__"))
                & (F.col("ch.__tpos__") == F.col("par.__ppos__")),
                "left",
            )
            .withColumn(out_col, F.col("__chosen__").cast(out_type))
            .drop(
                "__chosen__",
                "__u__",
                "low",
                "high",
                "__pcount__",
                "__tpos__",
                "__ppart_c__",
            )
        )
        return out
