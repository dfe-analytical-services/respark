from dataclasses import dataclass
from typing import Dict, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F, types as T


@dataclass(frozen=True, slots=True)
class ParentIndexArtifact:
    """
    parents_idx: (__ppart__, __ppos__, __pk__)
    part_cdf:    (__ppart__, low, high, __pcount__)
    total:       total parent rows (int)
    """

    parents_idx: DataFrame
    part_cdf: DataFrame
    total: int


def build_parent_index(df: DataFrame, value_col: str) -> ParentIndexArtifact:
    """
    Build distributed artifacts to sample uniformly from all values(df[value_col]).
    """
    parents = df.select(F.col(value_col).alias("__pk__"))

    # Partition-local index
    parents_pre = parents.select(
        "__pk__",
        F.spark_partition_id().alias("__ppart__"),
        F.monotonically_increasing_id().alias("__pmid__"),
    )
    w_part = Window.partitionBy("__ppart__").orderBy(F.col("__pmid__"))
    parents_idx = parents_pre.withColumn("__ppos__", F.row_number().over(w_part)).drop(
        "__pmid__"
    )

    # Partition sizes
    part_counts = parents_idx.groupBy("__ppart__").agg(
        F.max("__ppos__").alias("__pcount__")
    )

    # Single-row collect
    total = (
        part_counts.agg(F.sum("__pcount__").alias("total")).collect()[0]["total"] or 0
    )
    if total == 0:
        return ParentIndexArtifact(parents_idx.limit(0), part_counts.limit(0), 0)

    # Partition CDF over [0,1)
    cwin = Window.orderBy(F.col("__ppart__"))
    part_cdf = (
        part_counts.withColumn("__cum__", F.sum("__pcount__").over(cwin))
        .withColumn("high", F.col("__cum__") / F.lit(float(total)))
        .withColumn("low", F.lag("high", 1).over(cwin))
        .fillna({"low": 0.0})
        .select("__ppart__", "low", "high", "__pcount__")
    )

    return ParentIndexArtifact(parents_idx=parents_idx, part_cdf=part_cdf, total=total)


class DistributedChooser:

    def __init__(self):
        # Cache keys are logical identities—prefer names, not id(df)
        #   - parents: (pk_table, pk_column)
        #   - references: (ref_name, value_col)
        self._artifact_cache: Dict[Tuple[str, str], ParentIndexArtifact] = {}

    def ensure_artifact_for_parent(
        self,
        *,
        cache_key: Tuple[str, str],
        parent_df: DataFrame,
        value_col: str,
        distinct: bool = False,
    ) -> ParentIndexArtifact:
        """
        Build or fetch a ParentIndexArtifact for a 'parent' set of values.
        Set distinct=True to de-duplicate value_col first (e.g., 'set reuse').
        """
        art = self._artifact_cache.get(cache_key)
        if art is None:
            df = parent_df.select(value_col)
            if distinct:
                df = df.distinct()
            art = build_parent_index(df, value_col=value_col)
            self._artifact_cache[cache_key] = art
        return art

    def ensure_artifact_for_reference(
        self,
        cache_key: Tuple[str, str],
        reference_df: DataFrame,
        value_col: str,
    ) -> ParentIndexArtifact:
        """
        Convenience: references usually want DISTINCT semantics.
        """
        return self.ensure_artifact_for_parent(
            cache_key=cache_key,
            parent_df=reference_df,
            value_col=value_col,
            distinct=True,
        )

    def assign_uniform_from_artifact(
        self,
        child_df: DataFrame,
        artifact: ParentIndexArtifact,
        rng,
        out_col: str,
        out_type: T.DataType,
        salt_partition: str = "part",  # independent substream salts
        salt_position: str = "pos",
    ) -> DataFrame:
        """
        Assign child_df[out_col] by sampling uniformly from `artifact` (parent values),
        fully distributed and deterministic via RNG(row_idx, seed, salt).
        """
        if artifact.total == 0:
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
