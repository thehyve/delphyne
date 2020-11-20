from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Union, Optional

from sqlalchemy import Index, Table, PrimaryKeyConstraint, Constraint
from sqlalchemy.schema import DropConstraint, AddConstraint, DropIndex, CreateIndex

from .constraint_storage import ConstraintStorage
from .conventions import VOCAB_TABLES

if TYPE_CHECKING:
    from ..database import Database

logger = logging.getLogger(__name__)


class ConstraintManager:
    """
    Manager for adding and removing table constraints/indexes.

    This class can only operate on constraints that were defined in the
    SQLAlchemy table declarations. They can be dropped/added
    individually, per table, or for all non-vocabulary tables.

    Dropping does not cascade; meaning that if a constraint cannot be
    dropped because another object depends on it, an exception will be
    raised.
    """
    def __init__(self, database: Database):
        self._db = database
        self._storage = ConstraintStorage()
        self._table_lookup = {t.name: t for t in self._db.base.metadata.tables.values()}
        self._execution_options = {'schema_translate_map': self._db.schema_translate_map}

    def drop_cdm_constraints(self,
                             drop_constraint: bool = True,
                             drop_pk: bool = True,
                             drop_index: bool = True
                             ) -> None:
        """
        Remove constraints/indexes of all non-vocabulary tables.

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
        for table in self._db.base.metadata.tables.values():
            if table.name in VOCAB_TABLES:
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

        for index in indexes:
            self._drop_constraint_in_db(index)
        for constraint in constraints:
            self._drop_constraint_in_db(constraint)
        for pk in pks:
            self._drop_constraint_in_db(pk)

    def add_cdm_constraints(self,
                            add_constraint: bool = True,
                            add_pk: bool = True,
                            add_index: bool = True
                            ) -> None:
        """
        Add constraints/indexes of all non-vocabulary tables.

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
            indexes = self._storage.get_indexes()
        if add_pk:
            pks = self._storage.get_pks()
        if add_constraint:
            constraints = self._storage.get_constraints()

        for pk in pks:
            if pk.table.name not in VOCAB_TABLES:
                self._add_constraint_in_db(pk)
        for index in indexes:
            if index.table.name not in VOCAB_TABLES:
                self._add_constraint_in_db(index)
        for constraint in constraints:
            if constraint.table.name not in VOCAB_TABLES:
                self._add_constraint_in_db(constraint)

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
        table = self._get_table_by_name(table_name)
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

    def add_table_constraints(self,
                              table_name: str,
                              add_constraint: bool = True,
                              add_pk: bool = True,
                              add_index: bool = True
                              ) -> None:
        """
        Add constraints/indexes on a CDM table.

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
        constraints = self._storage.get_table_constraints(table_name)
        if not constraints:
            logger.warning(f'No constraints found that could be added '
                           f'for table "{table_name}"')
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

    def add_index(self, index: str) -> None:
        """
        Add a single index by name.

        :param index: str
            Name of the index as present in the database before it was
            dropped.
        :return: None
        """
        self._add_constraint_or_index(index)

    def add_constraint(self, constraint: str) -> None:
        """
        Add a single constraint by name.

        :param constraint: str
            Name of the constraint as present in the database before it
            was dropped.
        :return: None
        """
        self._add_constraint_or_index(constraint)

    def _add_constraint_or_index(self, constraint_name: str) -> None:
        constraint = self._storage.get_constraint(constraint_name)
        if constraint is None:
            raise ValueError(f'"{constraint_name}" not found')
        else:
            self._add_constraint_in_db(constraint)

    def _add_constraint_in_db(self, constraint: Union[Constraint, Index]):
        # Get a previously dropped constraint/index from the storage and
        # reapply it. If done successfully, remove it from the storage.
        with self._db.engine.connect().execution_options(**self._execution_options) as conn:
            logger.debug(f'Adding {constraint.name}')
            try:
                if isinstance(constraint, Index):
                    conn.execute(CreateIndex(constraint))
                else:
                    conn.execute(AddConstraint(constraint))
            except Exception as e:
                logger.error(e, exc_info=True)
                raise
            else:
                self._storage.remove_constraint(constraint.name)

    def drop_constraint(self, constraint_name: str) -> None:
        """
        Drop a single constraint by name.

        :param constraint_name: str
            Name of the constraint as present in the database.
        :return: None
        """
        constraint = self._get_active_constraint(constraint_name)
        if constraint is None:
            raise ValueError(f'Constraint "{constraint_name}" not found')
        else:
            self._drop_constraint_in_db(constraint)

    def drop_index(self, index_name: str) -> None:
        """
        Drop a single index by name.

        :param index_name: str
            Name of the index as present in the database.
        :return: None
        """
        index = self._get_active_index(index_name)
        if index is None:
            raise ValueError(f'Index "{index_name}" not found')
        else:
            self._drop_constraint_in_db(index)

    def _drop_constraint_in_db(self, constraint: Union[Constraint, Index]) -> None:
        # Drop a constraint/index. If successfully dropped, keep it in
        # the storage.
        with self._db.engine.connect().execution_options(**self._execution_options) as conn:
            logger.debug(f'Dropping {constraint.name}')
            try:
                if isinstance(constraint, Index):
                    conn.execute(DropIndex(constraint))
                else:
                    conn.execute(DropConstraint(constraint))
            except Exception as e:
                logger.error(e, exc_info=True)
                raise
            else:
                self._storage.store_constraint(constraint)

    def _get_active_constraint(self, name: str) -> Optional[Constraint]:
        # Search the tables for matching constraint name
        for table in self._db.base.metadata.tables.values():
            for constraint in table.constraints:
                if constraint.name == name:
                    return constraint
        return None

    def _get_active_index(self, name: str) -> Optional[Index]:
        # Search the tables for matching index name
        for table in self._db.base.metadata.tables.values():
            for index in table.indexes:
                if index.name == name:
                    return index
        return None

    def _get_table_by_name(self, table_name: str) -> Table:
        table = self._table_lookup.get(table_name)
        if table is None:
            raise KeyError(f'No table found with name "{table_name}"')
        return table
