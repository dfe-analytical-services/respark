from typing import Literal
from pyspark.sql import Column, types as T
from .base_rule import register_generation_rule, GenerationRule

TYPE_BITS = {
    "byte": 8,
    "short": 16,
    "int": 32,
    "long": 64,
}

TYPE_CAST = {
    "byte": T.ByteType(),
    "short": T.ShortType(),
    "int": T.IntegerType(),
    "long": T.LongType(),
}


class BaseIntegralRule(GenerationRule):

    spark_subtype: Literal["byte", "short", "int", "long"]

    def _signed_integral_bounds(self):
        """Return (min, max) for a signed integer with X bits.
        E.g 8-bit byte integral returns (-128, 127)
        """
        bits: int = TYPE_BITS[self.spark_subtype]
        min_value = -(1 << (bits - 1))
        max_value = (1 << (bits - 1)) - 1
        return (min_value, max_value)

    def generate_column(self) -> Column:
        default_min, default_max = self._signed_integral_bounds()
        min_value = self.params.get("min_value", default_min)
        max_value = self.params.get("max_value", default_max)

        rng = self.rng()
        col = rng.randint(min_value, max_value)
        return col.cast(TYPE_CAST[self.spark_subtype])


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
