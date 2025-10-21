from typing import Any, List
from pyspark.sql import Column, functions as F


def _to_col(x: Any) -> Column:
    """Convert a Python value or Column into a Column literal when needed."""
    return x if isinstance(x, Column) else F.lit(x)


def hash64(seed: int, row_idx: Column, *salts: Any) -> Column:
    """Compute a 64-bit xxhash from (seed, salts..., row_idx)."""
    parts: List[Column] = [F.lit(int(seed))]
    parts.extend(_to_col(s) for s in salts)
    parts.append(row_idx)
    return F.xxhash64(*parts)


def pmod_u64(col: Column, modulus: Column) -> Column:
    """Positive modulo for 64-bit hash values (safe for non-negative moduli)."""
    return F.pmod(col, modulus)
