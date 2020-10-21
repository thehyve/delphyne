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

import csv
import logging
import os
import subprocess
from collections import Counter, defaultdict
from datetime import datetime
from inspect import signature
from pathlib import Path
from typing import Callable, DefaultDict, Dict, Optional, List
from abc import ABC, abstractmethod
import itertools
import pandas as pd
from sqlalchemy.orm.session import Session

from .etl_stats import EtlTransformation, EtlStats
from ..database.database import Database

logger = logging.getLogger(__name__)


class OrmWrapper(ABC):
    """
    Wrapper which coordinates the execution of python ORM
    transformations.
    """
    def __init__(self, database: Database):
        self.db = database
        self.etl_stats = EtlStats()

        # {source_vocabulary_id: {source_code: target_concept_id}}
        self._stcm_lookup: DefaultDict[str, Dict[str, int]] = defaultdict(dict)

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

    @staticmethod
    def get_git_tag_or_branch() -> Optional[str]:
        """ Get the current git branch or tag using regular expressions
            TODO: get this information from ./.git/HEAD and ./.git/refs/tags/
        """
        # Run 'git branch' command
        try:
            branch_str = str(subprocess.check_output(['git', 'branch']))
        except subprocess.CalledProcessError:
            return

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

        transformation_metadata.end = datetime.now()
        logger.info(f'{statement.__name__} completed with success status: {transformation_metadata.query_success}')
        self.etl_stats.add_transformation(transformation_metadata)

    @staticmethod
    def _collect_query_statistics_bulk_mode(session: Session,
                                            records_to_insert: List,
                                            transformation_metadata: EtlTransformation
                                            ) -> None:
        # As SQLAlchemy's before_flush listener doesn't work in bulk
        # mode, only deleted and new objects in the record list are counted
        dc = Counter(Database.get_record_targets(session.deleted))
        transformation_metadata.deletion_counts = dc
        ic = Counter(Database.get_record_targets(records_to_insert))
        transformation_metadata.insertion_counts = ic

    def load_vocab_records_from_csv(self, source_file: Path, target_table) -> None:
        """
        Insert or update records in an OMOP vocabulary table, based on
        the contents of a csv file.

        :param source_file: Path
            csv file with header containing records of an OMOP
            vocabulary table
        :param target_table:
            SQLAlchemy vocabulary table class
        :return: None
        """
        # Note: inserts are one-by-one, so can be slow for large vocabulary files
        transformation_metadata = EtlTransformation(name=f'load_vocab_{source_file.name}')
        with source_file.open('r') as f_in, \
                self.db.session_scope(metadata=transformation_metadata) as session:
            rows = csv.DictReader(f_in)
            for row in rows:
                pk_col: str = target_table.__table__.primary_key.columns.values()[0].name
                record = session.query(target_table).get(row[pk_col])
                if not record:
                    record = target_table()

                # Set all variables
                for key, value in row.items():
                    setattr(record, key, value if value else None)

                session.add(record)
            transformation_metadata.end = datetime.now()
        self.etl_stats.add_transformation(transformation_metadata)

    def truncate_stcm_table(self):
        """Delete all records in the source_to_concept_map table."""
        logger.info('Truncating STCM table')
        with self.db.session_scope() as session:
            session.query(self.cdm.SourceToConceptMap).delete()

    def load_source_to_concept_map_from_csv(self, source_file: Path) -> None:
        """
        Insert STCM csv file into the STCM vocabulary table and add
        contents to stcm_lookup.

        :param source_file: Path
            csv file with header matching the CDM STCM columns
        :return: None
        """
        logger.info(f'Loading source to concept file: {str(source_file)}')
        transformation_metadata = EtlTransformation(name=f'load_{source_file.stem}')
        with self.db.session_scope(metadata=transformation_metadata) as session, source_file.open('r') as f_in:
            rows = csv.DictReader(f_in)

            first_row = next(rows)
            source_vocab = session.query(self.cdm.Vocabulary).get(first_row['source_vocabulary_id'])

            if not source_vocab:
                session.add(self.cdm.Vocabulary(
                    vocabulary_id=first_row['source_vocabulary_id'],
                    vocabulary_name=first_row['source_vocabulary_id'].replace('_', ' '),
                    vocabulary_reference='Active Biotech',
                    vocabulary_concept_id=0,
                ))

            for row in itertools.chain([first_row], rows):
                source_code = row['source_code']
                source_vocabulary_id = row['source_vocabulary_id']
                target_concept_id = int(row['target_concept_id'])
                # Skip unmapped records
                if target_concept_id == 0:
                    continue
                self._stcm_lookup[source_vocabulary_id][source_code] = target_concept_id
                session.add(self.cdm.SourceToConceptMap(**row))

            transformation_metadata.end = datetime.now()
            self.etl_stats.add_transformation(transformation_metadata)

    def lookup_stcm(self, source_vocabulary_id: str, source_code: str) -> int:
        """
        Get the target_concept_id for a given source code from the STCM.

        :param source_vocabulary_id: str
        :param source_code: str
        :return: int
            target_concept_id if present, otherwise 0
        """
        if source_vocabulary_id not in self._stcm_lookup:
            logger.warning('source_vocabulary "{}" not found'.format(source_vocabulary_id))
            return 0

        if pd.isnull(source_code):
            return 0

        return self._stcm_lookup[source_vocabulary_id].get(source_code, 0)
