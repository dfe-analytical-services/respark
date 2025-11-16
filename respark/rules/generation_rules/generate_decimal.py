from pyspark.sql import Column, functions as F, types as T
from ..rule_types import GenerationRule
from ..registry import register_generation_rule


@register_generation_rule("random_decimal")
class RandomDecimalRule(GenerationRule):

    def generate_column(self) -> Column:
        precision = int(self.params["precision"])
        scale = int(self.params["scale"])

        multiplier_int = 10**scale
        multiplier_dec_int = F.lit(multiplier_int).cast(T.DecimalType(38, 0))
        dec_bounds_type = T.DecimalType(38, scale)

        min_col = self.params.get("min_value_col")
        if min_col is None:
            min_value = self.params.get("min_value", "0")
            min_dec = F.lit(min_value).cast(dec_bounds_type)
        else:
            min_dec = F.col(min_col).cast(dec_bounds_type)

        max_col = self.params.get("max_value_col")
        if max_col is None:
            max_value = self.params.get("max_value", "1")
            max_dec = F.lit(max_value).cast(dec_bounds_type)
        else:
            max_dec = F.col(max_col).cast(dec_bounds_type)

        scaled_min = F.floor(min_dec * multiplier_dec_int).cast("long")
        scaled_max = F.floor(max_dec * multiplier_dec_int).cast("long")

        range_col = scaled_max - scaled_min

        rng = self.rng()
        offset = rng.uniform_int_inclusive(
            min_col=F.lit(0),
            max_col=range_col,
            salt="random_decimal_range",
        )

        scaled_value = (scaled_min + offset).cast("long")

        scaled_value_dec0 = scaled_value.cast(T.DecimalType(38, 0))
        scaled_value_with_scale = scaled_value_dec0.cast(T.DecimalType(38, scale))

        generated_dec = (scaled_value_with_scale / multiplier_dec_int).cast(
            T.DecimalType(precision, scale)
        )

        return generated_dec
