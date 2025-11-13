import re
from pyspark.sql import Column, functions as F

from ..rule_types import RelationalGenerationRule
from respark.rules.registry import register_generation_rule


@register_generation_rule("row_based_calculation")
class RowExpressionRule(RelationalGenerationRule):
    """
    sql_expression : A SQL expression compatible with pyspark.sql.functions.expr()
    """

    def generate_column(self) -> Column:
        sql_expression = self.params.get("sql_expression", "")
        return F.expr(sql_expression)

    def collect_parent_columns(self) -> set:
        distinct_parent_cols = set()
        sql_expression = self.params.get("sql_expression", "")

        distinct_parent_cols.update(re.findall(r"`([^`]+)`", sql_expression))

        return distinct_parent_cols
