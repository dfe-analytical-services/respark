from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class InternalColDepndency:
    """
    Define a dependency between two columns wihin the same table.
    Used to:
    - Determine column generation order
    - Validate fules that have inter_column relationships
    """

    parent_col: str
    child_col: str

    name: str = field(init=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "name",
            self.derive_name(self.parent_col, self.child_col),
        )

    @staticmethod
    def derive_name(parent_col: str, child_col: str) -> str:
        """
        Deterministic and collision-resistant default name.
        Includes columns to distinguish multiple FKs between the same tables.
        """
        return f"{parent_col}_REF_{child_col}"

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "parent_col": self.parent_col,
            "child_col": self.child_col,
        }


@dataclass(frozen=True, slots=True)
class FkConstraint:
    """
    Defines a foreign key constraint between two tables.

    fk_table.fk_column -> pk_table.pk_column

    Used to:
    - Determine generation order
    - Validate rules that have inter-table relationships.
    """

    pk_table: str
    pk_column: str
    fk_table: str
    fk_column: str

    name: str = field(init=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "name",
            self.derive_name(
                self.pk_table, self.pk_column, self.fk_table, self.fk_column
            ),
        )

    @staticmethod
    def derive_name(pk_table: str, pk_col: str, fk_table: str, fk_col: str) -> str:
        """
        Deterministic and collision-resistant default name.
        Includes columns to distinguish multiple FKs between the same tables.
        """
        return f"FK_{fk_table}_{fk_col}__REF__{pk_table}_{pk_col}"

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "pk_table": self.pk_table,
            "pk_column": self.pk_column,
            "fk_table": self.fk_table,
            "fk_column": self.fk_column,
        }
