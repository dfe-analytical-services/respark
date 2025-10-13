from decimal import Decimal
from pyspark.sql import Column, functions as F, types as T
from .base_rule import register_generation_rule, GenerationRule


@register_generation_rule("random_decimal")
class RandomDecimalRule(GenerationRule):
    def generate_column(self) -> Column:

        precision: int = self.params["precision"]
        scale: int = self.params["scale"]
        min_value: str = self.params["min_value"]
        max_value: str = self.params["max_value"]

        multiplier = 10**scale
        scaled_min = int(Decimal(min_value) * multiplier)
        scaled_max = int(Decimal(max_value) * multiplier)

        rng = self.rng()
        scaled = rng.rand_long(
            scaled_min, scaled_max, "random_decimal", precision, scale
        )

        scaled_dec = scaled.cast(T.DecimalType(38, 0))
        divisor = F.lit(multiplier).cast(T.DecimalType(38, 0))
        value_dec = scaled_dec / divisor
        return value_dec.cast(T.DecimalType(precision, scale))
