from __future__ import annotations

import logging
from copy import copy
from functools import lru_cache, wraps
from typing import TYPE_CHECKING, Union, Dict, Callable

from itertools import chain
from sqlalchemy import Index, Table, PrimaryKeyConstraint, Constraint, MetaData
from sqlalchemy.schema import DropConstraint, AddConstraint, DropIndex, CreateIndex

from .conventions import VOCAB_TABLES

if TYPE_CHECKING:
    from ..database import Database

# Two different MetaData objects are used within this module. The model
# metadata at Database.base.metadata, which contains the table
# definitions defined by the user in the template, and the reflected
# metadata at Database.reflected_metadata which contains table
# definitions of what is actually present in the database. Add-methods
# use the model metadata, while drop-methods use the reflected metadata.

logger = logging.getLogger(__name__)

ConstraintOrIndex = Union[Constraint, Index]


def _is_non_pk_constraint(constraint: ConstraintOrIndex) -> bool:
    # Return True if constraint is not an index or a PK
    is_constraint = isinstance(constraint, Constraint)
    is_pk = isinstance(constraint, PrimaryKeyConstraint)
    if is_constraint and not is_pk:
        return True
    return False


def _invalidate_db_cache(func: Callable) -> Callable:
    # Decorator to invalidate cached derivatives of reflected MetaData
    @wraps(func)
    def wrapper_invalidate_db_cache(*args, **kwargs):
        ConstraintManager.invalidate_current_db_cache()
        return func(*args, **kwargs)
    return wrapper_invalidate_db_cache


def _create_constraint_lookup(metadata: MetaData) -> Dict[str, ConstraintOrIndex]:
    lookup = dict()
    for table in metadata.tables.values():
        for constraint in chain(table.constraints, table.indexes):
            lookup[constraint.name] = constraint
    return lookup


class _TargetModel:
    """Lookup class for table properties of the target model."""
    def __init__(self, metadata: MetaData):
        self.table_lookup = {t.name: t for t in metadata.tables.values()}
        self.constraint_lookup = _create_constraint_lookup(metadata)
        self.indexes = [c for c in self.constraint_lookup.values()
                        if isinstance(c, Index)]
        self.pks = [c for c in self.constraint_lookup.values()
                    if isinstance(c, PrimaryKeyConstraint)]
        # All non-pk model constraints
        self.constraints = [c for c in self.constraint_lookup.values()
                            if _is_non_pk_constraint(c)]

    def is_model_table(self, table_name: str) -> bool:
        # Check table exists within the CDM model MetaData instance
        return table_name in self.table_lookup.keys()


class ConstraintManager:
    """
    Manager for adding and removing table constraints/indexes.

    They can be dropped/added individually, per table, or for all
    non-vocabulary tables.

    Dropping does not cascade; meaning that if a constraint cannot be
    dropped because another object depends on it, it will remain active.
    """
    def __init__(self, database: Database):
        self._db = database
        self._execution_options = {'schema_translate_map': self._db.schema_translate_map}
        self._model = _TargetModel(metadata=database.base.metadata)

    @property
    @lru_cache()
    def _reflected_constraint_lookup(self) -> Dict[str, ConstraintOrIndex]:
        return _create_constraint_lookup(self._db.reflected_metadata)

    @property
    @lru_cache()
    def _reflected_table_lookup(self) -> Dict[str, Table]:
        return {t.name: t for t in self._db.reflected_metadata.tables.values()}

    @staticmethod
    def invalidate_current_db_cache() -> None:
        logger.debug('Invalidating database tables cache')
        ConstraintManager._reflected_table_lookup.fget.cache_clear()
        ConstraintManager._reflected_constraint_lookup.fget.cache_clear()

    def drop_cdm_constraints(self,
                             drop_constraint: bool = True,
                             drop_pk: bool = True,
                             drop_index: bool = True
                             ) -> None:
        """
        Remove constraints/indexes of all non-vocabulary tables.

        Any table bound to your SQLAlchemy Base that is not an official
        CDM vocabulary table will be affected. If there are any
        additional tables in your database that are not bound to Base,
        they will not be affected by this method.

        :param drop_constraint: bool, default True
            If True, drop any FK, unique and check constraints.
        :param drop_pk: bool, default True
            If True, drop all PKs.
        :param drop_index: bool, default True
            If True, drop all indexes.
        :return: None
        """
        logger.info('Dropping CDM constraints')
        # Because we don't know in which order the table constraints can
        # be dropped without violating one in the process, we first
        # collect all of them, then drop them in the following order:
        # indexes, non-pk constraints, pks.
        constraints, pks, indexes = [], [], []
        for table in self._db.reflected_metadata.tables.values():
            if table.name in VOCAB_TABLES or not self._model.is_model_table(table.name):
                continue

            for constraint in table.constraints:
                is_pk = isinstance(constraint, PrimaryKeyConstraint)
                if is_pk and drop_pk:
                    pks.append(constraint)
                elif not is_pk and drop_constraint:
                    constraints.append(constraint)

            if drop_index:
                for index in table.indexes:
                    indexes.append(index)

        for constraint in chain(indexes, constraints, pks):
            self._drop_constraint_in_db(constraint)

    @_invalidate_db_cache
    def add_cdm_constraints(self,
                            add_constraint: bool = True,
                            add_pk: bool = True,
                            add_index: bool = True
                            ) -> None:
        """
        Add constraints/indexes of all non-vocabulary tables.

        Any table bound to your SQLAlchemy Base that is not an official
        CDM vocabulary table will be affected. If there are any
        additional tables in your database that are not bound to Base,
        they will not be affected by this method.

        If some constraints are already present before this method is
        called, they will remain active and not be added a second time,
        regardless of constraint names.

        :param add_constraint: bool, default True
            If True, add all FK, unique and check constraints.
        :param add_pk: bool, default True
            If True, add all PKs.
        :param add_index: bool, default True
            If True, add all indexes.
        :return: None
        """
        logger.info('Adding CDM constraints')
        constraints, pks, indexes = [], [], []
        if add_index:
            indexes = self._model.indexes
        if add_pk:
            pks = self._model.pks
        if add_constraint:
            constraints = self._model.constraints

        for constraint in chain(indexes, pks, constraints):
            if constraint.table.name not in VOCAB_TABLES:
                self._add_constraint_in_db(constraint)

    @_invalidate_db_cache
    def drop_table_constraints(self,
                               table_name: str,
                               drop_constraint: bool = True,
                               drop_pk: bool = True,
                               drop_index: bool = True
                               ) -> None:
        """
        Remove constraints/indexes of a CDM table.

        :param table_name: str
            Name of the table, without schema name.
        :param drop_constraint: bool, default True
            If True, drop any FK, unique and check constraints.
        :param drop_pk: bool, default True
            If True, drop the PK.
        :param drop_index: bool, default True
            If True, drop all indexes.
        :return: None
        """
        logger.info(f'Dropping constraints on table {table_name}')
        table = self._reflected_table_lookup.get(table_name)
        if table is None:
            raise KeyError(f'No table found in database with name "{table_name}"')

        if drop_index:
            for index in table.indexes:
                self._drop_constraint_in_db(index)
        if drop_constraint:
            for constraint in table.constraints:
                if not isinstance(constraint, PrimaryKeyConstraint):
                    self._drop_constraint_in_db(constraint)
        if drop_pk and table.primary_key:
            self._drop_constraint_in_db(table.primary_key)

    @_invalidate_db_cache
    def add_table_constraints(self,
                              table_name: str,
                              add_constraint: bool = True,
                              add_pk: bool = True,
                              add_index: bool = True
                              ) -> None:
        """
        Add constraints/indexes on a CDM table.

        This method requires that the table is bound to your SQLAlchemy
        Base and exists in the database.

        If some constraints are already present on the table before this
        method is called, they will remain active and not be added a
        second time, regardless of constraint names.

        :param table_name: str
            Name of the table, without schema name.
        :param add_constraint: bool, default True
            If True, add any FK, unique and check constraints.
        :param add_pk: bool, default True
            Add the table's PK if there is one.
        :param add_index: bool, default True
            Add all table indexes.
        :return: None
        """
        logger.info(f'Adding constraints on table {table_name}')
        if not self._model.is_model_table(table_name):
            raise KeyError(f'No table found in model with name "{table_name}"')

        table_constraints = [c for c in self._model.constraint_lookup.values()
                             if c.table.name == table_name]

        for constraint in table_constraints:
            if not add_index and isinstance(constraint, Index):
                continue
            if not add_pk and isinstance(constraint, PrimaryKeyConstraint):
                continue
            if not add_constraint and _is_non_pk_constraint(constraint):
                continue
            self._add_constraint_in_db(constraint)

    @_invalidate_db_cache
    def drop_constraint(self, constraint_name: str) -> None:
        """
        Drop a single constraint by name.

        This can be either a PK, FK, not null, unique or check
        constraint.

        :param constraint_name: str
            Name of the constraint as it exists in the database.
        :return: None
        """
        constraint = self._reflected_constraint_lookup.get(constraint_name)
        if constraint is None:
            raise KeyError(f'Constraint "{constraint_name}" not found')
        else:
            self._drop_constraint_in_db(constraint)

    @_invalidate_db_cache
    def drop_index(self, index_name: str) -> None:
        """
        Drop a single index by name.

        :param index_name: str
            Name of the index as it exists in the database.
        :return: None
        """
        index = self._reflected_constraint_lookup.get(index_name)
        if index is None:
            raise KeyError(f'Index "{index_name}" not found')
        else:
            self._drop_constraint_in_db(index)

    @_invalidate_db_cache
    def add_constraint(self, constraint: str) -> None:
        """
        Add a single constraint by name.

        This can be either a PK, FK, not null, unique or check
        constraint.

        :param constraint: str
            Name of the constraint as it exists in the model.
        :return: None
        """
        self._add_constraint_or_index(constraint)

    @_invalidate_db_cache
    def add_index(self, index: str) -> None:
        """
        Add a single index by name.

        :param index: str
            Name of the index as it exists in the model.
        :return: None
        """
        self._add_constraint_or_index(index)

    def _add_constraint_or_index(self, constraint_name: str) -> None:
        constraint = self._model.constraint_lookup.get(constraint_name)
        if constraint is None:
            raise KeyError(f'"{constraint_name}" not found')
        else:
            self._add_constraint_in_db(constraint)

    def _add_constraint_in_db(self, constraint: ConstraintOrIndex) -> None:
        if self._constraint_already_active(constraint):
            return
        with self._db.engine.connect().execution_options(**self._execution_options) as conn:
            logger.debug(f'Adding {constraint.name}')
            if isinstance(constraint, Index):
                conn.execute(CreateIndex(constraint))
            else:
                # We add a copy instead of the original constraint.
                # Otherwise, when you later call metadata.create_all to
                # create tables, SQLAlchemy thinks the constraints have
                # already been created and skips them.
                c = copy(constraint)
                conn.execute(AddConstraint(c))

    def _constraint_already_active(self, new_constraint: ConstraintOrIndex) -> bool:
        base_message = f'Cannot add {type(new_constraint).__name__} "{new_constraint.name}"'
        if new_constraint.name in self._reflected_constraint_lookup:
            logger.info(f'{base_message}, a relationship with this name already exists')
            return True
        for constraint in self._reflected_constraint_lookup.values():
            if self._constraints_functionally_equal(constraint, new_constraint):
                logger.info(f'{base_message}, a functional equivalent already exists '
                            f'with name "{constraint.name}"')
                return True
        return False

    def _drop_constraint_in_db(self, constraint: ConstraintOrIndex) -> None:
        # SQLAlchemy reflects empty PK objects in tables that don't have
        # a PK (anymore). These cannot be dropped because they have
        # no name and are therefore ignored here.
        if constraint.name is None:
            return

        with self._db.engine.connect().execution_options(**self._execution_options) as conn:
            logger.debug(f'Dropping {constraint.name}')
            if isinstance(constraint, Index):
                conn.execute(DropIndex(constraint))
            else:
                conn.execute(DropConstraint(constraint))

    @staticmethod
    def _constraints_functionally_equal(c1: ConstraintOrIndex,
                                        c2: ConstraintOrIndex,
                                        ) -> bool:
        # Check if two constraints/indexes are functional equivalents.
        # This assumes that if they act on the same table and columns
        # (and same column order), they are identical. This works for
        # all regular CDM constraints, but could fall short on custom
        # constraints.
        if not type(c1) == type(c2):
            return False
        if not c1.table.name == c2.table.name:
            return False
        if not [c.name for c in c1.columns] == [c.name for c in c2.columns]:
            return False
        return True
