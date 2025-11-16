from pyspark.sql import Column, functions as F
from ..rule_types import GenerationRule
from ..registry import register_generation_rule


def _reduce_precision(epoch_col: Column, precision: int | None) -> Column:
    """
    If precision is provided, floor epoch micros to that resolution.
    Example: frac_precision=3 -> keep milliseconds (resolution=1000 us).
    """
    if precision is None:
        return epoch_col
    if precision == 6:
        return epoch_col

    resolution = 10 ** (6 - precision)

    return (F.floor(epoch_col / F.lit(resolution)) * F.lit(resolution)).cast("long")


@register_generation_rule("random_date")
class RandomDateRule(GenerationRule):

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

        offset = rng.uniform_int_inclusive(
            min_col=F.lit(0), max_col=days_range, salt="something_for_now"
        )
        return F.date_add(min_date_col, offset).cast("date")


@register_generation_rule("random_timestamp_ltz")
class RandomTimestampLTZ(GenerationRule):

    def generate_column(self) -> Column:
        min_epoch_col = self.params.get("min_epoch_col")
        max_epoch_col = self.params.get("max_epoch_col")
        precision = self.params.get("precision")

        if min_epoch_col is None:
            min_epoch_micros = self.params.get("min_epoch_micros", "1577836800000000")
            min_epoch_col = F.lit(min_epoch_micros).cast("long")

        if max_epoch_col is None:
            max_epoch_micros = self.params.get("max_epoch_micros", "1767225599999999")
            max_epoch_col = F.lit(max_epoch_micros).cast("long")

        timespan_range = max_epoch_col - min_epoch_col

        rng = self.rng()
        offset = rng.uniform_int_inclusive(
            min_col=F.lit(0), max_col=timespan_range, salt="random_timestamp_ltz_range"
        )

        epoch_col = (min_epoch_col + offset).cast("long")
        timestamp_reduced_prec = _reduce_precision(
            epoch_col=epoch_col, precision=precision
        )

        return F.timestamp_micros(timestamp_reduced_prec)


@register_generation_rule("random_timestamp_ntz")
class RandomTimestampNTZ(GenerationRule):

    def generate_column(self) -> Column:
        min_epoch_col = self.params.get("min_epoch_col")
        max_epoch_col = self.params.get("max_epoch_col")
        precision = self.params.get("precision")

        if min_epoch_col is None:
            min_epoch_micros = self.params.get("min_epoch_micros", "1577836800000000")
            min_epoch_col = F.lit(min_epoch_micros).cast("long")

        if max_epoch_col is None:
            max_epoch_micros = self.params.get("max_epoch_micros", "1767225599999999")
            max_epoch_col = F.lit(max_epoch_micros).cast("long")

        timespan_range = max_epoch_col - min_epoch_col

        rng = self.rng()
        offset = rng.uniform_int_inclusive(
            min_col=F.lit(0), max_col=timespan_range, salt="random_timestamp_ntz_range"
        )

        epoch_col = (min_epoch_col + offset).cast("long")
        timestamp_reduced_prec = _reduce_precision(
            epoch_col=epoch_col, precision=precision
        )
        ts_ltz = F.timestamp_micros(timestamp_reduced_prec)
        ts_iso = F.date_format(ts_ltz, "yyyy-MM-dd HH:mm:ss.SSSSSS")

        return F.to_timestamp_ntz(ts_iso)
