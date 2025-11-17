import string
from pyspark.sql import Column, functions as F, types as T
from ..rule_types import GenerationRule
from ..registry import register_generation_rule
from respark.random import choice


def _string_to_char_array(string_col_expr: Column) -> Column:
    arr = F.split(string_col_expr.cast("string"), "")
    return F.slice(arr, 2, F.size(arr))  # drop leading ""


@register_generation_rule("random_string")
class RandomStringRule(GenerationRule):

    def generate_column(self) -> Column:

        min_length_col = self.params.get("min_length_col")
        if min_length_col is None:
            min_length = self.params.get("min_length", 0)
            min_length_col = F.lit(min_length).cast("int")

        max_length_col = self.params.get("max_length_col")
        if max_length_col is None:
            max_length = self.params.get("max_length", 50)
            max_length_col = F.lit(max_length).cast("int")

        ascii_lower: bool = self.params.get("ascii_lower", True)
        ascii_upper: bool = self.params.get("ascii_upper", True)
        digits: bool = self.params.get("digits", False)
        punctuation: bool = self.params.get("punctuation", False)

        extra_char_col = self.params.get("extra_char_col")
        if extra_char_col is None:
            extra_char = self.params.get("extra_char", "")
            extra_char_col = F.lit(extra_char).cast("string")

        char_options_set = ""

        if ascii_lower:
            char_options_set += string.ascii_lowercase
        if ascii_upper:
            char_options_set += string.ascii_uppercase
        if digits:
            char_options_set += string.digits
        if punctuation:
            char_options_set += string.punctuation

        char_options_array = F.array([F.lit(c) for c in char_options_set])
        extra_char_options_array = _string_to_char_array(extra_char_col)

        possible_char_array = F.array_distinct(
            F.concat(char_options_array, extra_char_options_array)
        )

        rng = self.rng()

        length = rng.uniform_int_inclusive(
            min_col=min_length_col,
            max_col=max_length_col,
            salt="random_string_length",
        )

        pos_seq = F.sequence(F.lit(0), F.lit(length - 1))
        chars = F.transform(
            pos_seq, lambda p: choice(rng, possible_char_array, "pos", p)
        )

        return F.concat_ws("", chars).cast(T.StringType())
