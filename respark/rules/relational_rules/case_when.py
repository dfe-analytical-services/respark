import re
from typing import Any, Set, Dict, List, Optional, TYPE_CHECKING
from dataclasses import dataclass
from pyspark.sql import Column, functions as F
from respark.rules.rule_types import RelationalGenerationRule
from respark.rules.registry import register_generation_rule, get_generation_rule

if TYPE_CHECKING:
    from respark.runtime import ResparkRuntime


@dataclass(slots=True)
class ThenAction:
    """
    A dataclass to describe the action taken if a condition is met.
    The action will either be a SQL expression, or a reference to a
    substitue rule to be used instead.

    Hence this class must contain then_expr XOR then_rule parameter.

    Attributes:

    then_expr:      A SQL string that can be evualuated by F.expr()
                    e.g "NULL", "`current_score` + `"

    then_rule:      A rule name of a registered GenerationRule to use to
                    produce a column instead

    then_params:    If then_rule is used, optional params to be passed to
                    the substituted rule.
    """

    then_expr: Optional[str] = None
    then_rule: Optional[str] = None
    then_params: Optional[Dict[str, Any]] = None

    def validate_action(self) -> None:
        has_expr = self.then_expr is not None
        has_rule = self.then_rule is not None
        if has_expr == has_rule:
            raise ValueError(
                "ThenAction requires 'then_expr' or 'then_rule', not both or neither."
            )


@dataclass(slots=True)
class WhenThenConditional:
    """
    A small dataclass to house the WHEN part of a CASE WHEN.

    Attributes:

    when_clause :   A SQL predicate e.g "`is_current_employee` IS NULL`

    then_action :   The action to take if the when_clause is met.
                    Held as ThenAction dataclass
    """

    when_clause: str  # SQL predicate, e.g. "`A` = 1" or "`A` IS NULL"
    then_action: ThenAction


@dataclass(slots=True)
class DefaultCase:
    """
    Even smaller data class to house the default action if no conditions are met
    during a CASE WHEN.

    Attributes:

    then_action :  The action to take if no when clauses are met.
                   Held as ThenAction dataclass.
    """

    then: ThenAction


@register_generation_rule("case_when_else")
class CaseWhenRule(RelationalGenerationRule):
    """
    A column generation rule that works like a SQL CASE ... WHEN.

    Possible outcomes (branches) are given as WhenThenConditional dataclasses,
    which define a "WHEN condition THEN action".

    Branches are evaluated in order; first match wins
    and the winning match used to generate the column.

    """

    def _build_subrule_col(
        self, rule_name: str, extra_params: Dict[str, Any], salt: int
    ) -> Column:
        """
        After a condition is satisfied, an alternative rule may
        be called if a expression is not passed.

        E.g: WHEN field "employment_end_date" is NULL:
                - generate using "random_date" using passed params.

        This internal method allows for injected params (__seed, __row_idx etc.)
        to be passed on to the sub rule, as if the sub rule was called from the plan.
        """
        base_params = dict(self.params)
        base_params.update(extra_params or {})
        base_params["__seed"] = int(base_params["__seed"]) + int(salt)
        sub = get_generation_rule(rule_name, **base_params)
        return sub.generate_column()

    def generate_column(self) -> Column:
        branches: List[WhenThenConditional] = self.params.get("branches", [])
        default_case: Optional[DefaultCase] = self.params.get("default_case")

        if not branches and not default_case:
            raise ValueError(
                "This rules requires at least one when' branch or an 'else' branch"
            )

        output: Optional[Column] = None

        # When valid branches are passed, build a sequence of .when() to assess in turn
        for idx, branch in enumerate(branches):

            cond_col = F.expr(branch.when_clause)

            if branch.then_action.then_expr is not None:
                then_col = F.expr(branch.then_action.then_expr)

            else:
                assert branch.then_action.then_rule is not None
                then_col = self._build_subrule_col(
                    branch.then_action.then_rule,
                    branch.then_action.then_params or {},
                    salt=idx + 1,
                )

            output = (
                F.when(cond_col, then_col)
                if output is None
                else output.when(cond_col, then_col)
            )

        # If no valid WHEN branches are passed, use the default case if present
        if output is None:
            if default_case and default_case.then.then_expr is not None:
                output = F.expr(default_case.then.then_expr)
            elif default_case and default_case.then.then_rule is not None:
                output = self._build_subrule_col(
                    default_case.then.then_rule,
                    default_case.then.then_params or {},
                    salt=0,
                )
            else:
                # If no default case given, along with no valid WHEN clauses, just return NULL
                output = F.lit(None)
        else:
            # But if you have WHEN cases, then finish with .otherwise() to complete chain of .when()
            if default_case and default_case.then.then_expr is not None:
                output = output.otherwise(F.expr(default_case.then.then_expr))
            elif default_case and default_case.then.then_rule is not None:
                output = output.otherwise(
                    self._build_subrule_col(
                        default_case.then.then_rule,
                        default_case.then.then_params or {},
                        salt=0,
                    )
                )
            else:
                output = output.otherwise(F.lit(None))

        return output

    def collect_parent_columns(self) -> Set[str]:
        branches: List[WhenThenConditional] = self.params.get("branches", [])

        distinct_parent_cols = set()

        for when_condition in branches:
            distinct_parent_cols.update(
                re.findall(r"`([^`]+)`", when_condition.when_clause)
            )

        return distinct_parent_cols
