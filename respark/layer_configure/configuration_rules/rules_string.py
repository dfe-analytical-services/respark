import string
from pyspark.sql import Column, functions as F, types as T
from .base_rule import register_generation_rule, GenerationRule


# String Rules
@register_generation_rule("random_string")
class RandomStringRule(GenerationRule):
    def generate_column(self) -> Column:
        min_length = self.params.get("min_length", 0)
        max_length = self.params.get("max_length", 50)
        charset = self.params.get("charset", string.ascii_letters)

        rng = self.rng()

        length = rng.rand_int(min_length, max_length, "len")
        charset_arr = F.array([F.lit(c) for c in charset])

        pos_seq = F.sequence(F.lit(0), F.lit(max_length - 1))
        chars = F.transform(pos_seq, lambda p: rng.choice(charset_arr, "pos", p))

        return F.concat_ws("", F.slice(chars, 1, length)).cast(T.StringType())
