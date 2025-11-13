import string
from pyspark.sql import Column, functions as F, types as T
from ..rule_types import GenerationRule
from ..registry import register_generation_rule
from respark.random import randint_int, choice


@register_generation_rule("random_string")
class RandomStringRule(GenerationRule):
    def generate_column(self) -> Column:
        min_length: int = self.params.get("min_length", 0)
        max_length: int = self.params.get("max_length", 50)
        ascii_lower: bool = self.params.get("ascii_lower", True)
        ascii_upper: bool = self.params.get("ascii_upper", True)
        extra_char: str = self.params.get("extra_char", "")

        char_set = ""

        if ascii_lower:
            char_set += string.ascii_lowercase
        if ascii_upper:
            char_set += string.ascii_uppercase
        if extra_char:
            char_set += extra_char

        rng = self.rng()

        length = randint_int(rng, min_length, max_length, char_set)
        charset_arr = F.array([F.lit(c) for c in char_set])

        pos_seq = F.sequence(F.lit(0), F.lit(max_length - 1))
        chars = F.transform(pos_seq, lambda p: choice(rng, charset_arr, "pos", p))

        return F.concat_ws("", F.slice(chars, 1, length)).cast(T.StringType())
