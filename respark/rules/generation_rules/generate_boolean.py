from pyspark.sql import Column, functions as F, types as T
from ..rule_types import GenerationRule
from ..registry import register_generation_rule


@register_generation_rule("random_boolean")
class RandomBooleanRule(GenerationRule):
    """
    Generate a random `BooleanType` value

    For each row in a column that has a non-`NULL` value,
    probability of `True` being generated, p, is set by passed parameter.

    E.g if p = 0.25, rule has 25% chance of generating a `True` value.

    Rule-specific `params` keys:
    ----------
    p_true : _float_
      Optional value in [0,1], otherwise defaults to 0.5

    p_true_col : _Column_
                Optional value in [0,1], otherwise uses `p_true`

    Behaviour
    --------
    - If value is not to be generated as `NULL`:
        - Uses deterministic per-row RNG to generate a value in [0,1).
        - Generates value (rng < p) as boolean.
    """

    def generate_column(self) -> Column:
        p_true_col = self.params.get("p_true_col")

        if p_true_col is None:
            p_true = self.params.get("p_true", 0.5)
            p_true_col = F.lit(p_true).cast("float")

        rng = self.rng()

        return (rng.uniform_double_01("bool") < p_true_col).cast(T.BooleanType())
