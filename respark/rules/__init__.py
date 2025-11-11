import importlib
from importlib import import_module, resources
import pkgutil
from .rules_registry import (
    GenerationRule,
    register_generation_rule,
    get_generation_rule,
    GENERATION_RULES_REGISTRY,
)

from .relational_rules.case_when import ThenAction, WhenThenConditional, DefaultCase

def auto_import_rules():
    pkg = importlib.import_module(__name__)
    skip_basenames = {"__init__"}

    for modinfo in pkgutil.walk_packages(pkg.__path__, prefix=f"{pkg.__name__}."):
        _, modname, _ = modinfo
        base = modname.rsplit(".", 1)[-1]
        if base in skip_basenames:
            continue
        import_module(modname)

auto_import_rules()
