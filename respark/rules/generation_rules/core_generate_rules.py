from pyspark.sql import functions as F
from ..rule_types import GenerationRule
from ..registry import register_generation_rule


@register_generation_rule("const_literal")
class ConstLiteralRule(GenerationRule):
    """
    A simple rule to allow populating a column with one expected field
    """

    def generate_column(self):
        return F.lit(self.params["value"])
