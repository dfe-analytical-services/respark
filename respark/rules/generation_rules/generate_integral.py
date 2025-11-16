from typing import Literal
from pyspark.sql import Column, functions as F
from ..rule_types import GenerationRule
from ..registry import register_generation_rule
from respark.core import INTEGRAL_BOUNDS, INTEGRAL_CAST


class BaseIntegralRule(GenerationRule):

    spark_subtype: Literal["byte", "short", "int", "long"]

    def generate_column(self) -> Column:
        default_min = INTEGRAL_BOUNDS[self.spark_subtype]["min_value"]
        default_max = INTEGRAL_BOUNDS[self.spark_subtype]["max_value"]

        min_value_col = self.params.get("min_value_col")
        if min_value_col is None:
            min_value = float(self.params.get("min_value", default_min))
            min_value_col = F.lit(min_value)

        max_value_col = self.params.get("max_value_col")
        if max_value_col is None:
            max_value = float(self.params.get("max_value", default_max))
            max_value_col = F.lit(max_value)

        rng = self.rng()

        offset = rng.uniform_long_inclusive(
            min_col=min_value_col,
            max_col=max_value_col,
            salt=f"random_{self.spark_subtype}_range",
        )

        col = (min_value_col + offset).cast("long")
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
