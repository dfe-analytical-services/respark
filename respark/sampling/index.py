from dataclasses import dataclass
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


@dataclass(frozen=True, slots=True)
class ParentIndexArtifact:
    """Distributed index and partition CDF for parent values.

    This artifact contains the distributed structures required for fast,
    uniform, deterministic sampling of parent values without collecting them
    to the driver.

    Attributes:
        parents_idx (DataFrame):
            One row per parent value with a partition-local position. Columns:
            - `__ppart__` (int): Spark partition ID of the parent row.
            - `__ppos__`  (int): 1-based position within its partition.
            - `__pk__`    (any): The parent value (copied from `parent_col`).

        part_cdf (DataFrame):
            The partition-level cumulative distribution function that tiles
            [0, 1] with ranges proportional to each partition's size. Columns:
            - `__ppart__`   (int): Spark partition ID.
            - `low`       (double): Inclusive lower bound of the partition range.
            - `high`      (double): Exclusive upper bound of the partition range.
            - `__pcount__`   (int): Number of parent rows in the partition.

        total (int):
            Total number of parent rows across all partitions (i.e., sum of
            `__pcount__`).

    """

    parents_idx: DataFrame
    part_cdf: DataFrame
    total: int


def build_parent_index(df: DataFrame, parent_col: str) -> ParentIndexArtifact:
    """Build the distributed index and partition CDF for uniform sampling.

    Constructs a `ParentIndexArtifact` that enables uniform sampling over all
    values in `df[parent_col]` by indexing each parent row with a partition ID
    and a partition-local position, and by computing a broadcastable CDF over
    partition sizes.

    Args:
        df (DataFrame):
            Input DataFrame that contains the parent values to sample from.
        parent_col (str):
            Column name in `df` whose values will be sampled.

    Returns:
        ParentIndexArtifact:
            An artifact with:
              - `parents_idx`: (`__ppart__`, `__ppos__`, `__pk__`)
              - `part_cdf`: (`__ppart__`, `low`, `high`, `__pcount__`)
              - `total`: total parent row count (int)

            If there are zero parent rows, the returned DataFrames are empty
            and `total` is 0.

    Examples:
        >>> artifact = build_parent_index(parent_df, "parent_id")
        >>> artifact.total
        4000000
        >>> artifact.part_cdf.orderBy("__ppart__").show()
        +--------+-----+----+-----------+
        |__ppart__|  low|high|__pcount__|
        +--------+-----+----+-----------+
        |       0|0.000|0.25|    1000000|
        |       1|0.250|0.50|    1000000|
        |       2|0.500|0.75|    1000000|
        |       3|0.750|1.00|    1000000|
        +--------+-----+----+-----------+

    """

    parent_values = df.select(F.col(parent_col).alias("__pk__"))

    # Assign partition id and a stable intra-partition order key
    parents_pre = parent_values.select(
        "__pk__",
        F.spark_partition_id().alias("__ppart__"),
        F.monotonically_increasing_id().alias("__pmid__"),
    )

    # Local 1..N position per N-partitions
    w_part = Window.partitionBy("__ppart__").orderBy(F.col("__pmid__"))
    parents_idx = parents_pre.withColumn("__ppos__", F.row_number().over(w_part)).drop(
        "__pmid__"
    )

    # Partition sizes
    partition_counts = parents_idx.groupBy("__ppart__").agg(
        F.max("__ppos__").alias("__pcount__")
    )

    total = (
        partition_counts.agg(F.sum("__pcount__").alias("total")).collect()[0]["total"]
        or 0
    )
    if total == 0:
        return ParentIndexArtifact(parents_idx.limit(0), partition_counts.limit(0), 0)

    # Partition CDF over [0, 1)
    cwin = Window.orderBy(F.col("__ppart__"))
    part_cdf = (
        partition_counts.withColumn("__cum__", F.sum("__pcount__").over(cwin))
        .withColumn("high", F.col("__cum__") / F.lit(float(total)))
        .withColumn("low", F.lag("high", 1).over(cwin))
        .fillna({"low": 0.0})
        .select("__ppart__", "low", "high", "__pcount__")
    )

    return ParentIndexArtifact(parents_idx=parents_idx, part_cdf=part_cdf, total=total)
