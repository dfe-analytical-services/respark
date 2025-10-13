from importlib import import_module
from pkgutil import iter_modules
from pathlib import Path

from .base_rule import (
    GenerationRule,
    register_generation_rule,
    get_generation_rule,
    GENERATION_RULES_REGISTRY,
)


def _auto_import_rules():
    pkg_dir = Path(__file__).parent
    pkg_name = __name__
    skip = {"__init__", "_core", "random_helpers"}
    for m in iter_modules([str(pkg_dir)]):
        if m.ispkg or m.name in skip:
            continue
        import_module(f"{pkg_name}.{m.name}")


_auto_import_rules()
