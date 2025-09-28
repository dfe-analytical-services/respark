from dataclasses import dataclass

@dataclass(slots=True)
class ColumnProfile:
    name: str
    normalised_type: str
    nullable: bool