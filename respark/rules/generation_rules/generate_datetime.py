from datetime import datetime
from pyspark.sql import Column, functions as F
from ..rule_types import GenerationRule
from ..registry import register_generation_rule
from respark.random import randint_int, randint_long


# Date Rules
@register_generation_rule("random_date")
class RandomDateRule(GenerationRule):
    def generate_column(self) -> Column:
        min_iso = self.params.get("min_iso", "2000-01-01")
        max_iso = self.params.get("max_iso", "2025-12-31")

        min_date = datetime.strptime(min_iso, "%Y-%m-%d")
        max_date = datetime.strptime(max_iso, "%Y-%m-%d")
        days_range = (max_date - min_date).days

        rng = self.rng()

        start = F.lit(min_iso).cast("date")
        offset = randint_int(rng, 0, days_range)
        return F.date_add(start, offset).cast("date")


@register_generation_rule("random_timestamp_ltz")
class RandomTimestampLTZ(GenerationRule):
    def generate_column(self) -> Column:
        min_epoch_micros = self.params.get("min_epoch_micros", "1577836800000000")
        max_epoch_micros = self.params.get("max_epoch_micros", "1767225599999999")

        timespan_range = int(max_epoch_micros) - int(min_epoch_micros)
        rng = self.rng()

        offset = randint_long(rng, 0, timespan_range)
        return F.timestamp_micros(F.lit(min_epoch_micros).cast("long") + offset)


@register_generation_rule("random_timestamp_ntz")
class RandomTimestampNTZ(GenerationRule):
    def generate_column(self) -> Column:
        min_epoch_micros = self.params.get("min_epoch_micros", "1577836800000000")
        max_epoch_micros = self.params.get("max_epoch_micros", "1767225599999999")

        timespan_range = int(max_epoch_micros) - int(min_epoch_micros)
        rng = self.rng()

        offset = randint_long(rng, 0, timespan_range)
        timestamp_ltz = F.timestamp_micros(
            F.lit(min_epoch_micros).cast("long") + offset
        )

        timestamp_iso = F.date_format(timestamp_ltz, "yyyy-MM-dd HH:mm:ss.SSSSSS")
        return F.to_timestamp_ntz(timestamp_iso)
