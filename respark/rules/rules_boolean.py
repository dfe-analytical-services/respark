from pyspark.sql import Column, functions as F, types as T
from .base_rule import register_generation_rule, GenerationRule


@register_generation_rule("random_boolean")
class RandomBooleanRule(GenerationRule):
    def generate_column(self) -> Column:

        percentage_true = float(self.params.get("percentage_true", 0.5))
        rng = self.rng()

        return (rng.uniform_01_double("bool") < F.lit(percentage_true)).cast(
            T.BooleanType()
        )
