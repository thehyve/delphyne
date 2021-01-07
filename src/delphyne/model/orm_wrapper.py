# Copyright 2019 The Hyve
#
# Licensed under the GNU General Public License, version 3,
# or (at your option) any later version (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.gnu.org/licenses/
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

import logging
import os
from abc import ABC, abstractmethod
from collections import Counter
from functools import lru_cache
from inspect import signature
from typing import Callable, List

from sqlalchemy.orm.session import Session

from .etl_stats import EtlTransformation, etl_stats
from ..database import Database, events

logger = logging.getLogger(__name__)


class OrmWrapper(ABC):
    """
    Wrapper which coordinates the execution of python ORM
    transformations.
    """
    def __init__(self, database: Database):
        self.db = database

    @property
    @abstractmethod
    def cdm(cls):
        return NotImplementedError('Missing class variable: cdm')

    def run(self):
        """Run ETL procedure"""
        raise NotImplementedError('Method is not implemented')

    @staticmethod
    def is_git_repo() -> bool:
        return os.path.exists('./.git')

    def execute_transformation(self, statement: Callable, bulk: bool = False) -> None:
        """
        Execute an ETL transformation via a python statement (function
        that will be called).

        :param statement: Callable
            python function which takes this wrapper as input
        :param bulk: bool
            Use SQLAlchemy's bulk_save_objects instead of add_all for
            persisting the ORM objects
        """
        logger.info(f'Executing transformation: {statement.__name__}')
        transformation_metadata = EtlTransformation(name=statement.__name__)

        with self.db.session_scope(raise_on_error=False,
                                   metadata=transformation_metadata) as session:
            func_args = signature(statement).parameters
            records_to_insert = statement(self, session) if 'session' in func_args else statement(self)
            logger.info(f'Saving {len(records_to_insert)} objects')
            if bulk:
                session.bulk_save_objects(records_to_insert)
                self._collect_query_statistics_bulk_mode(session, records_to_insert, transformation_metadata)
            else:
                session.add_all(records_to_insert)

        transformation_metadata.end_now()
        logger.info(f'{statement.__name__} completed with success status: {transformation_metadata.query_success}')
        etl_stats.add_transformation(transformation_metadata)

    @staticmethod
    def _collect_query_statistics_bulk_mode(session: Session,
                                            records_to_insert: List,
                                            transformation_metadata: EtlTransformation
                                            ) -> None:
        # As SQLAlchemy's before_flush listener doesn't work in bulk
        # mode, only deleted and new objects in the record list are
        # counted
        dc = Counter(events.get_record_targets(session.deleted))
        transformation_metadata.deletion_counts = dc
        ic = Counter(events.get_record_targets(records_to_insert))
        transformation_metadata.insertion_counts = ic

    @lru_cache(maxsize=50000)
    def lookup_stcm(self, source_vocabulary_id: str, source_code: str) -> int:
        """
        Query the STCM table to get the target_concept_id.

        :param source_vocabulary_id: str
        :param source_code: str
        :return: int
            target_concept_id if present, otherwise 0
        """
        with self.db.session_scope() as session:
            q = session.query(self.cdm.SourceToConceptMap)
            result = q.filter_by(source_vocabulary_id=source_vocabulary_id,
                                 source_code=source_code).one_or_none()
            if result is None:
                return 0
            return result.target_concept_id
