from __future__ import annotations

import logging
from functools import lru_cache, wraps
from typing import TYPE_CHECKING, Union, Dict, List, Callable

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


def _invalidate_db_cache(func: Callable) -> Callable:
    @wraps(func)
    def wrapper_invalidate_db_cache(*args, **kwargs):
        ConstraintManager.invalidate_current_db_cache()
        return func(*args, **kwargs)
    return wrapper_invalidate_db_cache


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

        self._model_constraint_lookup = self._create_constraint_lookup(self._db.base.metadata)
        self._model_indexes = [c for c in self._model_constraint_lookup.values()
                               if isinstance(c, Index)]
        self._model_pks = [c for c in self._model_constraint_lookup.values()
                           if isinstance(c, PrimaryKeyConstraint)]
        # All non-pk model constraints
        self._model_constraints = [c for c in self._model_constraint_lookup.values()
                                   if not (isinstance(c, Index)
                                           or isinstance(c, PrimaryKeyConstraint))]
        self._model_table_lookup = {t.name: t for t in self._db.base.metadata.tables.values()}

    # @property
    # @lru_cache()
    # def _model_constraint_lookup(self) -> Dict[str, ConstraintOrIndex]:
    #     return self._create_constraint_lookup(self._db.base.metadata)

    @property
    @lru_cache()
    def _current_db_constraint_lookup(self) -> Dict[str, ConstraintOrIndex]:
        return self._create_constraint_lookup(self._db.reflected_metadata)

    # @property
    # @lru_cache()
    # def _model_indexes(self) -> List[Index]:
    #     return [c for c in self._model_constraint_lookup.values()
    #             if isinstance(c, Index)]
    #
    # @property
    # @lru_cache()
    # def _model_pks(self) -> List[PrimaryKeyConstraint]:
    #     return [c for c in self._model_constraint_lookup.values()
    #             if isinstance(c, PrimaryKeyConstraint)]
    #
    # @property
    # @lru_cache()
    # def _model_constraints(self) -> List[Constraint]:
    #     # All non-pk model constraints
    #     constraints = []
    #     for c in self._model_constraint_lookup.values():
    #         if isinstance(c, Index) or isinstance(c, PrimaryKeyConstraint):
    #             continue
    #         constraints.append(c)
    #     return constraints

    # @property
    # @lru_cache()
    # def _model_table_lookup(self) -> Dict[str, Table]:
    #     return {t.name: t for t in self._db.base.metadata.tables.values()}

    @property
    @lru_cache()
    def _current_db_table_lookup(self) -> Dict[str, Table]:
        return {t.name: t for t in self._db.reflected_metadata.tables.values()}

    @staticmethod
    def invalidate_current_db_cache() -> None:
        logger.debug('Invalidating database tables cache')
        ConstraintManager._current_db_table_lookup.fget.cache_clear()
        ConstraintManager._current_db_constraint_lookup.fget.cache_clear()

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
        constraints, pks, indexes = [], [], []
        for table in self._db.reflected_metadata.tables.values():
            if table.name in VOCAB_TABLES or not self._is_model_table(table.name):
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
        Constraints that are already present in any of the CDM tables
        will be ignored.

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
            indexes = self._model_indexes
        if add_pk:
            pks = self._model_pks
        if add_constraint:
            constraints = self._model_constraints

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
        table = self._get_db_table_by_name(table_name)
        if drop_index:
            for index in table.indexes:
                self._drop_constraint_in_db(index)
        if drop_constraint:
            for constraint in table.constraints:
                if not isinstance(constraint, PrimaryKeyConstraint):
                    self._drop_constraint_in_db(constraint)
        if drop_pk:
            if table.primary_key:
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
        Base and is present in the database.

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
        if self._model_table_lookup.get(table_name) is None:
            logger.warning(f'Table not found in model: "{table_name}"')
            return

        constraints = [c for c in self._model_constraint_lookup.values()
                       if c.table.name == table_name]

        for constraint in constraints:
            is_index = isinstance(constraint, Index)
            is_pk = isinstance(constraint, PrimaryKeyConstraint)
            if is_index and not add_index:
                continue
            if is_pk and not add_pk:
                continue
            if not (is_index or is_pk) and not add_constraint:
                continue
            self._add_constraint_or_index(constraint.name)

    @_invalidate_db_cache
    def drop_constraint(self, constraint_name: str) -> None:
        """
        Drop a single constraint by name.

        :param constraint_name: str
            Name of the constraint as present in the database.
        :return: None
        """
        constraint = self._current_db_constraint_lookup.get(constraint_name)
        if constraint is None:
            raise ValueError(f'Constraint "{constraint_name}" not found')
        else:
            self._drop_constraint_in_db(constraint)

    @_invalidate_db_cache
    def drop_index(self, index_name: str) -> None:
        """
        Drop a single index by name.

        :param index_name: str
            Name of the index as present in the database.
        :return: None
        """
        index = self._current_db_constraint_lookup.get(index_name)
        if index is None:
            raise ValueError(f'Index "{index_name}" not found')
        else:
            self._drop_constraint_in_db(index)

    @_invalidate_db_cache
    def add_constraint(self, constraint: str) -> None:
        """
        Add a single constraint by name.

        :param constraint: str
            Name of the constraint as present in the model.
        :return: None
        """
        self._add_constraint_or_index(constraint)

    @_invalidate_db_cache
    def add_index(self, index: str) -> None:
        """
        Add a single index by name.

        :param index: str
            Name of the index as present in the model.
        :return: None
        """
        self._add_constraint_or_index(index)

    def _add_constraint_or_index(self, constraint_name: str) -> None:
        constraint = self._model_constraint_lookup.get(constraint_name)
        if constraint is None:
            raise ValueError(f'"{constraint_name}" not found')
        else:
            self._add_constraint_in_db(constraint)

    def _add_constraint_in_db(self, constraint: ConstraintOrIndex) -> None:
        if self._constraint_already_present(constraint):
            return
        with self._db.engine.connect().execution_options(**self._execution_options) as conn:
            logger.debug(f'Adding {constraint.name}')
            if isinstance(constraint, Index):
                conn.execute(CreateIndex(constraint))
            else:
                conn.execute(AddConstraint(constraint))

    def _constraint_already_present(self, new_constraint: ConstraintOrIndex) -> bool:
        base_message = f'Cannot add {type(new_constraint).__name__} "{new_constraint.name}"'
        if new_constraint.name in self._current_db_constraint_lookup:
            logger.info(f'{base_message}, a relationship with this name already exists')
            return True
        for constraint in self._current_db_constraint_lookup.values():
            if self._constraints_functionally_equal(constraint, new_constraint):
                logger.info(f'{base_message}, a functional equivalent already exists '
                            f'with name "{constraint.name}"')
                return True
        return False

    def _drop_constraint_in_db(self, constraint: ConstraintOrIndex) -> None:
        with self._db.engine.connect().execution_options(**self._execution_options) as conn:
            logger.debug(f'Dropping {constraint.name}')
            if isinstance(constraint, Index):
                conn.execute(DropIndex(constraint))
            else:
                conn.execute(DropConstraint(constraint))

    def _get_db_table_by_name(self, table_name: str) -> Table:
        table = self._current_db_table_lookup.get(table_name)
        if table is None:
            raise KeyError(f'No table found in database with name "{table_name}"')
        return table

    def _is_model_table(self, table_name: str) -> bool:
        # Check table exists within the CDM model MetaData instance
        return table_name in self._model_table_lookup.keys()

    @staticmethod
    def _create_constraint_lookup(metadata: MetaData) -> Dict[str, ConstraintOrIndex]:
        lookup = dict()
        for table in metadata.tables.values():
            for constraint in chain(table.constraints, table.indexes):
                lookup[constraint.name] = constraint
        return lookup

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
