from typing import Optional, TYPE_CHECKING
from pyspark.sql import DataFrame, Column, functions as F
from respark.rules import GenerationRule, register_generation_rule

if TYPE_CHECKING:
    from respark.runtime import ResparkRuntime


@register_generation_rule("row_based_calculation")
class RowExpressionRule(GenerationRule):
    """
    sql_expression : A SQL expression compatible with pyspark.sql.functions.expr()
    """

    def generate_column(self) -> Column:
        sql_expression = self.params["sql_expression"]
        return F.expr(sql_expression)

    def apply(
        self, base_df: DataFrame, runtime: Optional["ResparkRuntime"], target_col: str
    ) -> DataFrame:

        return base_df.withColumn(target_col, self.generate_column())
