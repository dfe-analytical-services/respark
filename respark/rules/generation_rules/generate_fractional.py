from typing import Literal
from pyspark.sql import Column, functions as F
from ..rule_types import GenerationRule
from ..registry import register_generation_rule
from respark.core import FRACTIONAL_BOUNDS, FRACTIONAL_TYPE


class BaseFractionalRule(GenerationRule):
    spark_subtype: Literal["float", "double"]

    def generate_column(self) -> Column:

        default_min, default_max = FRACTIONAL_BOUNDS[self.spark_subtype]

        min_value_col = self.params.get("min_value_col")
        if min_value_col is None:
            min_value = float(self.params.get("min_value", default_min))
            min_value_col = F.lit(min_value)

        max_value_col = self.params.get("max_value_col")
        if max_value_col is None:
            max_value = float(self.params.get("max_value", default_max))
            max_value_col = F.lit(max_value)

        offset = max_value_col - min_value_col

        rng = self.rng()
        u = rng.uniform_double_01(self.spark_subtype)
        col = min_value_col + u * offset

        return col.cast(FRACTIONAL_TYPE[self.spark_subtype]())


@register_generation_rule("random_float")
class RandomFloatRule(BaseFractionalRule):
    spark_subtype = "float"


@register_generation_rule("random_double")
class RandomDoubleRule(BaseFractionalRule):
    spark_subtype = "double"
