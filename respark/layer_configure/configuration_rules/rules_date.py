from datetime import datetime
from pyspark.sql import Column, functions as F, types as T
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
        offset = rng.randint(0, days_range)
        return F.date_add(F.to_date(F.lit(min_date_str)), offset).cast(T.DateType())
