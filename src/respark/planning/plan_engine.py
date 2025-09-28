from typing import Dict, Any, List
from dataclasses import dataclass, field, asdict
from respark.profiling import SchemaProfile


@dataclass
class ColumnGenerationPlan:
    name: str
    rule: str
    params: Dict[str, Any] = field(default_factory=dict)

@dataclass
class TableGenerationPlan:
    name: str
    row_count: int
    columns: List[ColumnGenerationPlan] = field(default_factory=list)

@dataclass
class SchemaGenerationPlan:
    tables: List[TableGenerationPlan] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
def make_generation_plan(schema_profile: SchemaProfile) -> SchemaGenerationPlan:
    tables : List[TableGenerationPlan] = []

    for _, table_profile in schema_profile.tables.items():
        col_plans: List[ColumnGenerationPlan] = []
        row_count = table_profile.row_count

        for _, column_profile in table_profile.columns.items():
            col_plans.append(ColumnGenerationPlan(
                name=column_profile.name,
                rule=column_profile.default_rule(),
                params=column_profile.type_specific_params()
            ))

        
        tables.append(TableGenerationPlan(
            name=table_profile.name,
            row_count=row_count,
            columns=col_plans
        ))


    return SchemaGenerationPlan(tables)
