import re
from typing import Any, Set, Dict, List, Optional, TYPE_CHECKING
from dataclasses import dataclass, field
from pyspark.sql import Column, functions as F
from respark.random.seeding import vary_seed
from respark.rules.rule_types import GenerationRule, RelationalGenerationRule
from respark.rules.registry import register_generation_rule, get_generation_rule

if TYPE_CHECKING:
    from respark.runtime import ResparkRuntime


@dataclass(slots=True)
class ThenRule:
    """
    A dataclass to hold the subtitute rule to apply if a condition is met.

    Attributes:

    then_rule:          A rule name of a registered GenerationRule to use to
                        produce a column instead

    then_rule_params:   Optional params to be passed to
                        the substituted rule.
    """

    then_rule: str
    then_rule_params: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class WhenThenConditional:
    """
    A small dataclass to house a possible rule application.
    WHEN {when_clause} THEN {then_rule} rule is used

    Attributes:

    when_clause :   A SQL predicate e.g "`is_current_employee` IS NULL`

    then_rule :   The action to take if the when_clause is met.
                    Held as ThenAction dataclass
    """

    when_clause: str  # SQL predicate, e.g. "`A` = 1" or "`A` IS NULL"
    then_rule: ThenRule


@register_generation_rule("rule_switch")
class RuleSwitch(RelationalGenerationRule):
    """
    A column generation rule that works like a SQL CASE ... WHEN.

    Possible outcomes (branches) are given as WhenThenConditional dataclasses,
    which define a "WHEN condition THEN rule".

    Branches are evaluated in order; first match wins
    and the winning match used to generate the column.

    """

    def _parse_WhenThenConditional(
        self, when_then_dict: Dict[str, Any]
    ) -> WhenThenConditional:

        # Validate branch inputs:
        required_branch_keys = {"when", "then"}
        input_branch_keys = set(when_then_dict.keys())

        missing_keys = required_branch_keys - input_branch_keys
        if missing_keys:
            raise KeyError(f"Branch is missing required key(s): {sorted(missing_keys)}")

        extra_keys = input_branch_keys - required_branch_keys
        if extra_keys:
            raise KeyError(f"Branch contains extra_keys key(s): {sorted(extra_keys)}")

        when_clause: str = when_then_dict["when"]

        # Validate then_rule ipiuts

        then_dict: Dict[str, Any] = when_then_dict["then"]

        if "rule_name" not in then_dict.keys():
            raise KeyError(f"Then dict requires 'rule_name' key")

        then_rule = then_dict["rule_name"]

        then_rule_params = {
            param: param_value
            for param, param_value in then_dict.items()
            if param != "rule_name"
        }

        when_then_object = WhenThenConditional(
            when_clause=when_clause,
            then_rule=ThenRule(then_rule=then_rule, then_rule_params=then_rule_params),
        )

        return when_then_object

    def _parse_default_rule(self, default_dict: Dict[str, Any]) -> ThenRule:

        try:
            then_rule = default_dict["rule_name"]
        except:
            raise KeyError("default dict does not have rule_name")

        then_rule_params = {
            param: param_value
            for param, param_value in default_dict.items()
            if param != "rule_name"
        }

        default_rule_object = ThenRule(
            then_rule=then_rule, then_rule_params=then_rule_params
        )

        return default_rule_object

    def _build_subrule(
        self, rule_name: str, extra_params: Dict[str, Any], *salt_tokens
    ) -> GenerationRule:
        """
        After a condition is satisfied, an alternative rule may
        be called if a expression is not passed.

        E.g: WHEN field `employment_end_date` is NULL:
                - generate using "random_date" using passed params.

        This internal method allows for injected params (__seed, __row_idx etc.)
        to be passed on to the sub rule, as if the sub rule was called from the plan.
        """
        base_params = {
            k: v
            for k, v in self.params.items()
            if k not in {"branches", "default", "when", "then", "rule_name"}
        }

        base_params.update(extra_params or {})
        base_params["__seed"] = vary_seed(base_params["__seed"], *salt_tokens)

        return get_generation_rule(rule_name, **base_params)

    def generate_column(self) -> Column:

        branches: List[WhenThenConditional] = [
            self._parse_WhenThenConditional(b) for b in self.params.get("branches", [])
        ]

        default_rule_object: ThenRule = self._parse_default_rule(
            self.params.get("default", {})
        )
        default_col = self._build_subrule(
            default_rule_object.then_rule,
            default_rule_object.then_rule_params,
            *default_rule_object.then_rule_params.values(),
        ).generate_column()

        output: Optional[Column] = None

        for idx, branch in enumerate(branches):
            cond_col = F.expr(branch.when_clause)

            sub_rule = self._build_subrule(
                branch.then_rule.then_rule,
                branch.then_rule.then_rule_params,
                *default_rule_object.then_rule_params.values(),
            )
            then_col = sub_rule.generate_column()

            output = (
                F.when(cond_col, then_col)
                if output is None
                else output.when(cond_col, then_col)
            )

        if output is None:
            output = default_col

        else:
            output = output.otherwise(default_col)

        return output

    def collect_parent_columns(self) -> Set[str]:

        branch_objects: List[WhenThenConditional] = [
            self._parse_WhenThenConditional(b) for b in self.params.get("branches", [])
        ]

        default_rule_object: ThenRule = self._parse_default_rule(
            self.params.get("default", {})
        )

        distinct_parent_cols: Set[str] = set()

        for branch in branch_objects:
            distinct_parent_cols.update(re.findall(r"`([^`]+)`", branch.when_clause))

            for param in branch.then_rule.then_rule_params.values():
                distinct_parent_cols.update(re.findall(r"`([^`]+)`", str(param)))

        for param in default_rule_object.then_rule_params.values():
            distinct_parent_cols.update(re.findall(r"`([^`]+)`", str(param)))

        self.params["parent_cols"] = distinct_parent_cols

        return distinct_parent_cols
