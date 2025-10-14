import math
import sys
from typing import Any, List, Union

from pyspark.sql import Column, functions as F, types as T


###
# Dervive type-specific limits and bounds
###
INTEGRAL_BITS = {
    "byte": 8,
    "short": 16,
    "int": 32,
    "long": 64,
}

INTEGRAL_CAST = {
    "byte": T.ByteType(),
    "short": T.ShortType(),
    "int": T.IntegerType(),
    "long": T.LongType(),
}

INTEGRAL_BOUNDS = {
    name: {
        "min_value": -(1 << (bits - 1)),
        "max_value": (1 << (bits - 1)) - 1,
    }
    for name, bits in INTEGRAL_BITS.items()
}

# IEEE-754 ranges
DOUBLE_MAX = sys.float_info.max
DOUBLE_MIN = -sys.float_info.max

FLOAT_MAX = math.ldexp(2.0 - 2.0**-23, 127)
FLOAT_MIN = -FLOAT_MAX

FRACTIONAL_BOUNDS = {
    "float": (FLOAT_MIN, FLOAT_MAX),
    "double": (DOUBLE_MIN, DOUBLE_MAX),
}

FRACTIONAL_CAST = {
    "float": T.FloatType(),
    "double": T.DoubleType(),
}

# Constants used to build uniform doubles with ~53 bits of precision.
_U53_INT = 1 << 53
_U53 = float(_U53_INT)


def _to_col(x: Any) -> Column:
    """Convert Python values or Column into a Column literal when needed."""
    return x if isinstance(x, Column) else F.lit(x)


class RNG:
    """
    Deterministic, per-row RNG based on a stable row index and a global seed.

    - row_idx: a deterministic, stable index Column (i.e., __row_idx).
    - base_seed: run-level seed, any int.

    Methords include optional *salt parameters. These allow independent random
    substreams by passing unique tokens: table name, column name, column type etc.
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
        return F.xxhash64(*parts)

    def uniform_01_double(self, *salt: Any) -> Column:
        """
        Uniform double in [0, 1) with ~53 bits of precision.
        Derived by taking the lower 53 bits of the 64-bit hash.
        """
        return (F.pmod(self._hash64(*salt), F.lit(_U53_INT)) / F.lit(_U53)).cast(
            "double"
        )

    def rand_long(self, min_value: int, max_value: int, *salt: Any) -> Column:
        """
        Takes a min and max value as limits, returns a column of randomly generated LongType
        """

        span = max_value - min_value + 1
        if span <= 0:
            raise ValueError(
                f"randint: invalid span computed from [{min_value}, {max_value}]."
            )

        span_col = F.lit(span).cast("long")
        return F.pmod(self._hash64(*salt), span_col) + F.lit(min_value)

    def rand_int(self, min_value: int, max_value: int, *salt: Any) -> Column:
        return self.rand_long(min_value, max_value, *salt).cast("int")

    def rand_short(self, min_value: int, max_value: int, *salt: Any) -> Column:
        return self.rand_long(min_value, max_value, *salt).cast("short")

    def rand_byte(self, min_value: int, max_value: int, *salt: Any) -> Column:
        return self.rand_long(min_value, max_value, *salt).cast("short")

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
