from typing import Literal
from pyspark.sql import Column
from .base_rule import register_generation_rule, GenerationRule
from .numeric_utils import INTEGRAL_BOUNDS, INTEGRAL_CAST


class BaseIntegralRule(GenerationRule):

    spark_subtype: Literal["byte", "short", "int", "long"]

    def generate_column(self) -> Column:
        default_min = INTEGRAL_BOUNDS[self.spark_subtype]["min_value"]
        default_max = INTEGRAL_BOUNDS[self.spark_subtype]["max_value"]
        min_value = self.params.get("min_value", default_min)
        max_value = self.params.get("max_value", default_max)

        rng = self.rng()
        col = rng.rand_int(min_value, max_value)
        return col.cast(INTEGRAL_CAST[self.spark_subtype])


@register_generation_rule("random_byte")
class RandomByteRule(BaseIntegralRule):
    spark_subtype = "byte"


@register_generation_rule("random_short")
class RandomShortRule(BaseIntegralRule):
    spark_subtype = "short"


@register_generation_rule("random_int")
class RandomIntRule(BaseIntegralRule):
    spark_subtype = "int"


@register_generation_rule("random_long")
class RandomLongRule(BaseIntegralRule):
    spark_subtype = "long"
