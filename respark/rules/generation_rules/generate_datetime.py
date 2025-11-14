from datetime import datetime
from pyspark.sql import Column, functions as F
from ..rule_types import GenerationRule
from ..registry import register_generation_rule
from respark.random import randint_int, randint_long


# Date Rules
@register_generation_rule("random_date")
class RandomDateRule(GenerationRule):
    """
    Generate a random T.Datetype between inclusive min/max bounds.
    Uses a literal value if provided via column profiling.
    """

    def generate_column(self) -> Column:

        min_date_col = self.params.get("min_date_col")
        max_date_col = self.params.get("max_date_col")

        if min_date_col is None:
            min_iso = self.params.get("min_iso", "2000-01-01")
            min_date_col = F.lit(min_iso).cast("date")

        if max_date_col is None:
            max_iso = self.params.get("max_iso", "2025-12-31")
            max_date_col = F.lit(max_iso).cast("date")

        days_range = F.datediff(max_date_col, min_date_col)

        rng = self.rng()

        start = F.lit(min_iso).cast("date")
        
        offset = rng.uniform_int_inclusive(
            min_col=F.lit(0),
            max_col= days_range,
            salt="something_for_now")
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
