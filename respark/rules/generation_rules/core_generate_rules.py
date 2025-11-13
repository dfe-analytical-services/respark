from typing import List, Any
from pyspark.sql import Column, functions as F
from ..rule_types import GenerationRule
from ..registry import register_generation_rule


@register_generation_rule("const_literal")
class ConstLiteralRule(GenerationRule):
    """
    A simple rule to allow populating a column with one expected field
    """

    def generate_column(self) -> Column:
        return F.lit(self.params["value"])


@register_generation_rule("random_from_set")
class RandomFromSet(GenerationRule):
    """
    A simple rule to allow populating a column with from a predermined set of
    allowed values.

    E.g For column `employment_status` from valid set of strings values:
        ['Pre-employment Checks', 'Current', 'Not Current']
    """

    def generate_column(self) -> Column:
        valid_options: List[Any] = self.params.get("valid_options", [])

        if not valid_options:
            raise ValueError("random_from_set requires non-empty 'valid_options'.")

        n = len(valid_options)

        rng = self.rng()
        u = rng.uniform_01_double(self.seed, self.row_idx)

        idx = F.floor(u * F.lit(n)).cast("int")

        options_array = F.array(*[F.lit(v) for v in valid_options])
        return F.element_at(options_array, idx + F.lit(1))
