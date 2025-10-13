from typing import Any, Iterable, List, Union
from pyspark.sql import Column, functions as F


# Constants used for building IEEE-754 doubles with ~53 bits of precision.
_U53_INT = 1 << 53
_U53 = float(_U53_INT)


def _to_col(x: Any) -> Column:
    """Convert Python values or Column into a Column literal when needed."""
    return x if isinstance(x, Column) else F.lit(x)


class RNG:
    """
    Deterministic, per-row RNG built from a stable row index, a base seed, and optional 'salt'.
    - `row_idx` must be deterministic and stable across runs/partitions.
    - `seed` controls the global stream.
    - `salt` lets callers create independent sub-streams (e.g., by column name).
    """

    def __init__(self, row_idx: Column, base_seed: int):
        self.row_idx = row_idx
        self.seed = int(base_seed)

    def _hash64(self, *salt: Any) -> Column:
        """
        Create a 64-bit hash Column from (seed, salt..., row_idx).
        """
        parts: List[Column] = [F.lit(self.seed)]
        parts.extend(_to_col(s) for s in salt)
        parts.append(self.row_idx)
        return F.xxhash64(*parts)  # returns a signed 64-bit integer Column

    def u01(self, *salt: Any) -> Column:
        """
        Uniform double in [0, 1) with ~53 bits of precision.
        Derived by taking the lower 53 bits of the 64-bit hash.
        """
        return (F.pmod(self._hash64(*salt), F.lit(_U53_INT)) / F.lit(_U53)).cast(
            "double"
        )

    def randint(self, low: int, high: int, *salt: Any) -> Column:
        """
        Uniform integer in the inclusive range [low, high], returned as a LONG Column.
        (Callers can cast down to int/short/byte safely after ensuring bounds.)
        """
        low_i = int(low)
        high_i = int(high)
        if high_i < low_i:
            raise ValueError(f"randint: high ({high}) < low ({low}).")

        # span = high - low + 1  (as long)
        span = high_i - low_i + 1
        if span <= 0:
            # Protect against overflow if high - low + 1 wraps (very large values)
            # This is extremely unlikely for sane ranges, but we guard anyway.
            raise ValueError(f"randint: invalid span computed from [{low}, {high}].")

        span_col = F.lit(span).cast("long")
        return (F.pmod(self._hash64(*salt), span_col) + F.lit(low_i)).cast("long")

    def choice(self, options: Union[List[Any], Column], *salt: Any) -> Column:
        """
        Choose uniformly from a Python list or a Column array.
        For Column arrays, supports per-row varying choices.
        """
        h = self._hash64(*salt)
        if isinstance(options, Column):
            arr = options
            arr_len = F.size(arr)
            # Guard: empty arrays would yield modulo by 0
            return F.when(
                arr_len > 0,
                F.element_at(arr, (F.pmod(h, arr_len) + F.lit(1)).cast("int")),
            ).otherwise(F.lit(None))
        else:
            if not options:
                return F.lit(None)
            arr = F.array([F.lit(v) for v in options])
            idx1 = (F.pmod(h, F.lit(len(options))) + F.lit(1)).cast(
                "int"
            )  # element_at is 1-based
            return F.element_at(arr, idx1)
