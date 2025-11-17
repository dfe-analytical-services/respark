from pyspark.sql import Column, functions as F, types as T
from ..rule_types import GenerationRule
from ..registry import register_generation_rule


@register_generation_rule("random_decimal")
class RandomDecimalRule(GenerationRule):

    def generate_column(self) -> Column:
        precision = int(self.params["precision"])
        scale = int(self.params["scale"])

        max_scale_multiplier = F.lit(10**scale).cast(T.DecimalType(38, scale))

        min_col = self.params.get("min_value_col")
        if min_col is None:
            min_value = self.params.get("min_value", "0")
            min_dec = F.lit(min_value).cast(T.DecimalType(38, scale))
        else:
            min_dec = F.col(min_col).cast(T.DecimalType(38, scale))

        max_col = self.params.get("max_value_col")
        if max_col is None:
            max_value = self.params.get("max_value", "1")
            max_dec = F.lit(max_value).cast(T.DecimalType(38, scale))
        else:
            max_dec = F.col(max_col).cast(T.DecimalType(38, scale))

        scaled_min = F.floor(min_dec * max_scale_multiplier)
        scaled_max = F.floor(max_dec * max_scale_multiplier)

        range_col = scaled_max - scaled_min

        rng = self.rng()
        offset = rng.uniform_long_inclusive(
            min_col=F.lit(0),
            max_col=range_col,
            salt="random_decimal_range",
        )

        scaled_value = scaled_min + offset

        generated_dec = (scaled_value / max_scale_multiplier).cast(
            T.DecimalType(precision, scale)
        )

        return generated_dec
