from __future__ import annotations

from typing import Union, Dict, List

from sqlalchemy import Index, Constraint, PrimaryKeyConstraint


class _NoOverwriteDict(dict):
    """
    Dictionary that doesn't allow a new value to be set for an
    existing key. This should never happen because the constraint names
    are supposed to be unique, but if it does happen a ValueError is
    raised.
    """
    def __setitem__(self, key, value):
        if key in self:
            raise ValueError(f'Key already exists "{key}"')
        super(_NoOverwriteDict, self).__setitem__(key, value)


class ConstraintStorage:
    """
    Storage for constraints that have been dropped from the CDM.

    Any constraint/index that has been dropped from the CDM will be
    stored in here. This makes it easy to later reapply them if needed.
    As soon as a constraint is reapplied, it will be removed from this
    storage.
    """
    def __init__(self):
        # {constraint_name: Constraint/Index}
        self._dropped_constraints: Dict[str, Union[Constraint, Index]] = _NoOverwriteDict()

    def get_constraint(self, constraint_name: str) -> Union[Constraint, Index, None]:
        return self._dropped_constraints.get(constraint_name)

    def store_constraint(self, constraint: Union[Constraint, Index]) -> None:
        self._dropped_constraints[constraint.name] = constraint

    def remove_constraint(self, constraint_name: str) -> None:
        """Remove a constraint from the storage by its name."""
        del self._dropped_constraints[constraint_name]

    def get_table_constraints(self, table_name: str):
        """Get all constraints/indexes of a table from the storage."""
        return [c for c in self._dropped_constraints.values() if c.table.name == table_name]

    def get_indexes(self) -> List[Index]:
        """Get all stored indexes."""
        return [i for i in self._dropped_constraints.values() if isinstance(i, Index)]

    def get_pks(self) -> List[PrimaryKeyConstraint]:
        """Get all stored PKs."""
        return [pk for pk in self._dropped_constraints.values()
                if isinstance(pk, PrimaryKeyConstraint)]

    def get_constraints(self) -> List[Constraint]:
        """Get all stored unique, check and FK constraints."""
        return [c for c in self._dropped_constraints.values()
                if isinstance(c, Constraint) and not isinstance(c, PrimaryKeyConstraint)]
