from typing import TYPE_CHECKING
from pyspark.sql import DataFrame, Column, functions as F

from ..rule_types import RelationalGenerationRule
from respark.rules.registry import register_generation_rule

if TYPE_CHECKING:
    from respark.runtime import ResparkRuntime


@register_generation_rule("row_based_calculation")
class RowExpressionRule(RelationalGenerationRule):
    """
    sql_expression : A SQL expression compatible with pyspark.sql.functions.expr()
    """

    def generate_column(self) -> Column:
        sql_expression = self.params["sql_expression"]
        return F.expr(sql_expression)

    def register_dependency(self):
        pass