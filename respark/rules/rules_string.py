import string
from pyspark.sql import Column, functions as F, types as T
from .core_rules import register_generation_rule, GenerationRule
from respark.random import randint_int, choice


# String Rules
@register_generation_rule("random_string")
class RandomStringRule(GenerationRule):
    def generate_column(self) -> Column:
        min_length = self.params.get("min_length", 0)
        max_length = self.params.get("max_length", 50)
        charset = self.params.get("charset", string.ascii_letters)

        rng = self.rng()

        length = randint_int(rng, min_length, max_length, "len")
        charset_arr = F.array([F.lit(c) for c in charset])

        pos_seq = F.sequence(F.lit(0), F.lit(max_length - 1))
        chars = F.transform(pos_seq, lambda p: choice(rng, charset_arr, "pos", p))

        return F.concat_ws("", F.slice(chars, 1, length)).cast(T.StringType())
