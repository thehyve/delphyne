"""ORM wrapper module."""

import logging
import os
from abc import ABC, abstractmethod
from collections import Counter
from functools import lru_cache
from inspect import signature
from typing import Callable, List

from sqlalchemy.orm.session import Session

from .etl_stats import EtlTransformation
from ..database import Database, events

logger = logging.getLogger(__name__)


class OrmWrapper(ABC):
    """
    Wrapper coordinating the execution of python ORM transformations.

    Parameters
    ----------
    database : Database
        Database instance to interact with.
    """

    def __init__(self, database: Database):
        self.db = database

    @property
    @abstractmethod
    def cdm(cls):
        """CDM module."""
        return NotImplementedError('Missing class variable: cdm')

    def run(self):
        """Run ETL procedure."""
        raise NotImplementedError('Method is not implemented')

    @staticmethod
    def is_git_repo() -> bool:
        """
        Check whether current working dir is a git repository.

        Returns
        -------
        bool
            Return True if CWD is a git repository.
        """
        return os.path.exists('./.git')

    def execute_batch_transformation(self, batch_statement: Callable, bulk: bool = False, batch_size: int = 10000) -> None:
        """
        Execute an ETL transformation in batches of given size via a python statement.

        Parameters
        ----------
        batch_statement : Callable
            Python generator function which takes this wrapper as input and yields one record at a time.
            It will be called as a transformation.
        bulk : bool
            If True, use SQLAlchemy's bulk_save_objects instead of
            add_all for persisting the ORM objects.
        batch_size : int
            Number of records inserted in each batch. At maximum this number of records is kept in memory.
            Smaller batch sizes will decrease memory use, bigger batch sizes will increase insert performance.

        Returns
        -------
        None
        """
        logger.info(f'Executing transformation in batches: {batch_statement.__name__} ')
        with self.db.tracked_session_scope(name=batch_statement.__name__, raise_on_error=False) \
                as (session, transformation_metadata):
            func_args = signature(batch_statement).parameters
            if 'session' in func_args:
                records_generator = batch_statement(self, session)
            else:
                records_generator = batch_statement(self)

            records_to_insert = []
            batch_count = 0
            total_records_inserted = 0
            for record in records_generator:
                records_to_insert.append(record)
                if len(records_to_insert) >= batch_size:
                    batch_count += 1
                    total_records_inserted += len(records_to_insert)
                    logger.info(f'Saving {len(records_to_insert)} objects (batch {batch_count}): '
                                f'{batch_statement.__name__} ')
                    if bulk:
                        session.bulk_save_objects(records_to_insert)
                        self._collect_query_statistics_bulk_mode(session, records_to_insert,
                                                                 transformation_metadata)
                    else:
                        session.add_all(records_to_insert)
                    # Both flushing and committing to write from memory to disk
                    session.commit()  # also flushes: https://docs.sqlalchemy.org/en/14/orm/session_basics.html#committing
                    records_to_insert = []

            # Insert any remaining records
            if len(records_to_insert) > 0:
                batch_count += 1
                total_records_inserted += len(records_to_insert)
                logger.info(f'Saving {len(records_to_insert)} objects (batch {batch_count}): '
                            f'{batch_statement.__name__} ')
                if bulk:
                    session.bulk_save_objects(records_to_insert)
                    self._collect_query_statistics_bulk_mode(session, records_to_insert,
                                                             transformation_metadata)
                else:
                    session.add_all(records_to_insert)
                session.commit()

            logger.info(f'{batch_statement.__name__} ({total_records_inserted} records) completed with success status: '
                        f'{transformation_metadata.query_success}')

    def execute_transformation(self, statement: Callable, bulk: bool = False) -> None:
        """
        Execute an ETL transformation via a python statement.

        Parameters
        ----------
        statement : Callable
            Python function which takes this wrapper as input and returns a list of records to be inserted.
            It will be called as a transformation.
        bulk : bool
            If True, use SQLAlchemy's bulk_save_objects instead of
            add_all for persisting the ORM objects.

        Returns
        -------
        None
        """
        logger.info(f'Executing transformation: {statement.__name__}')
        with self.db.tracked_session_scope(name=statement.__name__, raise_on_error=False) \
                as (session, transformation_metadata):
            func_args = signature(statement).parameters
            if 'session' in func_args:
                records_to_insert = statement(self, session)
            else:
                records_to_insert = statement(self)
            logger.info(f'Saving {len(records_to_insert)} objects')
            if bulk:
                session.bulk_save_objects(records_to_insert)
                self._collect_query_statistics_bulk_mode(session, records_to_insert,
                                                         transformation_metadata)
            else:
                session.add_all(records_to_insert)

            logger.info(f'{statement.__name__} completed with success status: '
                        f'{transformation_metadata.query_success}')

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

        Parameters
        ----------
        source_vocabulary_id : str
            Vocabulary ID of the source code.
        source_code : str
            Code belonging to the source vocabulary for which to look up
            the mapping.

        Returns
        -------
        int
            Target_concept_id if present, otherwise 0.
        """
        with self.db.session_scope() as session:
            q = session.query(self.cdm.SourceToConceptMap)
            result = q.filter_by(source_vocabulary_id=source_vocabulary_id,
                                 source_code=source_code).one_or_none()
            if result is None:
                return 0
            return result.target_concept_id
