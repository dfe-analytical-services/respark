from typing import Literal
from pyspark.sql import Column, functions as F
from .base_rule import register_generation_rule, GenerationRule
from .numeric_utils import FRACTIONAL_BOUNDS, FRACTIONAL_CAST


class BaseFractionalRule(GenerationRule):
    spark_subtype: Literal["float", "double"]

    def generate_column(self) -> Column:

        default_min, default_max = FRACTIONAL_BOUNDS[self.spark_subtype]
        min_value = float(self.params.get("min_value", default_min))
        max_value = float(self.params.get("max_value", default_max))

        rng = self.rng()
        u = rng.uniform_01_double(self.spark_subtype)
        col = F.lit(min_value) + u * F.lit(max_value - min_value)
        return col.cast(FRACTIONAL_CAST[self.spark_subtype])


@register_generation_rule("random_float")
class RandomFloatRule(BaseFractionalRule):
    spark_subtype = "float"


@register_generation_rule("random_double")
class RandomDoubleRule(BaseFractionalRule):
    spark_subtype = "double"
