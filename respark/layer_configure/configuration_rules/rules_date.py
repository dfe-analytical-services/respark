from datetime import datetime
from pyspark.sql import Column, functions as F
from .base_rule import register_generation_rule, GenerationRule


# Date Rules
@register_generation_rule("random_date")
class RandomDateRule(GenerationRule):
    def generate_column(self) -> Column:
        min_date_str = self.params.get("min_date", "2000-01-01")
        max_date_str = self.params.get("max_date", "2025-12-31")

        min_date = datetime.strptime(min_date_str, "%Y-%m-%d")
        max_date = datetime.strptime(max_date_str, "%Y-%m-%d")
        days_range = (max_date - min_date).days

        rng = self.rng()

        start = F.lit(min_date_str).cast("date")
        offset = rng.rand_int(0, days_range)
        return F.date_add(start, offset).cast("date")
