from typing import Any, List, Union
from pyspark.sql import Column, functions as F
from .rng import RNG


def randint_long(rng: RNG, min_value: int, max_value: int, *salt: Any) -> Column:
    """Uniform integer Column in [min_value, max_value] (inclusive) as LongType."""
    span = max_value - min_value + 1
    if span <= 0:
        raise ValueError(
            f"randint_long: invalid span computed from [{min_value}, {max_value}]."
        )

    span_col = F.lit(span).cast("long")
    return F.pmod(rng._hash64(*salt), span_col) + F.lit(min_value)


def randint_int(rng: RNG, min_value: int, max_value: int, *salt: Any) -> Column:
    """Uniform int Column in [min_value, max_value] (inclusive)."""
    return randint_long(rng, min_value, max_value, *salt).cast("int")


def randint_short(rng: RNG, min_value: int, max_value: int, *salt: Any) -> Column:
    """Uniform short Column in [min_value, max_value] (inclusive)."""
    return randint_long(rng, min_value, max_value, *salt).cast("short")


def randint_byte(rng: RNG, min_value: int, max_value: int, *salt: Any) -> Column:
    """Uniform byte Column in [min_value, max_value] (inclusive)."""
    return randint_long(rng, min_value, max_value, *salt).cast("byte")


def choice(rng: RNG, options: Union[List[Any], Column], *salt: Any) -> Column:
    """Choose uniformly from a Python list or an array Column."""
    h = rng._hash64(*salt)

    if isinstance(options, Column):
        arr = options
        arr_len = F.size(arr)
        return F.when(
            arr_len > 0,
            F.element_at(arr, (F.pmod(h, arr_len) + F.lit(1)).cast("int")),
        ).otherwise(F.lit(None))
    else:
        if not options:
            return F.lit(None)
        arr = F.array([F.lit(v) for v in options])
        idx1 = (F.pmod(h, F.lit(len(options))) + F.lit(1)).cast("int")
        return F.element_at(arr, idx1)
