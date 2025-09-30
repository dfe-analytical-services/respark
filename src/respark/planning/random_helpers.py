from typing import Any, List, Union
from pyspark.sql import Column, functions as F


class RNG:
    def __init__(self, row_idx: Column, base_seed: int):
        self.row_idx = row_idx
        self.seed = int(base_seed)
        self._U53 = float(1 << 53)
        self._U53_INT = 1 << 53

    def _u64(self, *salt: Any) -> Column:

        parts: List[Column] = [F.lit(self.seed)]
        for s in salt:
            parts.append(s if isinstance(s, Column) else F.lit(str(s)))
        parts.append(self.row_idx)
        return F.xxhash64(*parts)

    def u01(self, *salt: Any) -> Column:
        return (F.pmod(self._u64(*salt), F.lit(self._U53_INT)) / F.lit(self._U53)).cast(
            "double"
        )

    def randint(self, low: int, high: int, *salt: Any) -> Column:
        span = int(high) - int(low) + 1
        return (F.pmod(self._u64(*salt), F.lit(span)) + F.lit(int(low))).cast("int")

    def choice(self, options: Union[List[Any], Column], *salt: Any) -> Column:
        if isinstance(options, Column):
            arr = options
            arr_len_col = F.size(arr)
            idx1 = (F.pmod(self._u64(*salt), arr_len_col) + F.lit(1)).cast("int")
            return F.element_at(arr, idx1)
        else:
            arr = F.array([F.lit(v) for v in options])
            idx1 = (F.pmod(self._u64(*salt), F.lit(len(options))) + F.lit(1)).cast(
                "int"
            )
            return F.element_at(arr, idx1)
