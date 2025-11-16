from typing import Any
from pyspark.sql import Column, functions as F
from respark.core.numeric_types import INTEGRAL_BOUNDS
from .hashing import hash64

# Constants used to build uniform doubles with ~53 bits of precision.
_U53_INT = 1 << 53
_U53 = float(_U53_INT)


class RNG:
    """Deterministic, per-row RNG based on a stable row index and a global seed.

    Args:
        row_idx: A deterministic, stable per-row identity Column (e.g., a hash of key columns).
        base_seed: Run-level seed (any int). Changing it changes all downstream draws.

    Salts:
        All methods accept arbitrary `*salt` values (strings, Columns, ints).
        Using different salts yields independent substreams (e.g., "part", "pos").
    """

    def __init__(self, row_idx: Column, base_seed: int):
        self.row_idx = row_idx
        self.seed = int(base_seed)

    def _hash64(self, *salt: Any) -> Column:
        """Create a 64-bit hash Column from (seed, salt..., row_idx)."""
        return hash64(self.seed, self.row_idx, *salt)

    def uniform_double_01(self, *salt: Any) -> Column:
        """Uniform double in [0, 1) with ~53 bits of precision.

        Derived by taking the lower 53 bits of the 64-bit hash and scaling.
        """
        return (F.pmod(self._hash64(*salt), F.lit(_U53_INT)) / F.lit(_U53)).cast(
            "double"
        )

    def uniform_long_inclusive(
        self, min_col: Column, max_col: Column, salt: str
    ) -> Column:
        """
        Sample a LONG uniformly from [min_col, max_col], inclusive.
        Assumes caller handles nulls/invalids.
        """
        min_col = min_col.cast("long")
        max_col = max_col.cast("long")
        range_double = (max_col - min_col).cast("double")
        offset = F.floor(self.uniform_double_01(salt) * range_double).cast("long")
        return (min_col + offset).cast("long")

    def uniform_int_inclusive(
        self, min_col: Column, max_col: Column, salt: str
    ) -> Column:
        """
        Sample an INT uniformly from [min_col, max_col], inclusive.
        Delegates to the LONG variant after clamping bounds to int range.
        """
        INT_MIN = INTEGRAL_BOUNDS["int"]["min_value"]
        INT_MAX = INTEGRAL_BOUNDS["int"]["max_value"]

        min_col_clamped = F.greatest(min_col.cast("long"), F.lit(INT_MIN).cast("long"))
        max_col_clamped = F.least(max_col.cast("long"), F.lit(INT_MAX).cast("long"))

        sampled_long = self.uniform_long_inclusive(
            min_col_clamped, max_col_clamped, salt
        )
        return sampled_long.cast("int")
