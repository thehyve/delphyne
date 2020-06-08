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
from pathlib import Path
from types import ModuleType
from typing import Optional, Union, Dict, List

from sqlalchemy import Table
from sqlalchemy.schema import CreateSchema

from ._settings import default_sql_parameters
from .cdm._defaults import VOCAB_SCHEMA
from .database.database import Database
from .model.etl_stats import EtlStats
from .model.orm_wrapper import OrmWrapper
from .model.raw_sql_wrapper import RawSqlWrapper

logger = logging.getLogger(__name__)

_HERE = Path(__file__).parent


class Wrapper(OrmWrapper, RawSqlWrapper):
    """
    Task coordinator supporting the process of converting source data
    into the OMOP CDM.

    database : str or Database
        Either a URI string for connecting to a database or an already
        instantiated Database object.
    bulk : bool
        Insert ORM objects using SqlAlchemy's bulk_save_objects. This
        will improve ETL performance, but comes at a cost of less
        granular processing statistics.
    reports : bool
        Write two additional tsv files with detailed information on
        source data counts and ETL transformations.
    cdm : module
        A module from the cdm package which contains the OMOP ORM table
        definitions.
    sql_parameters : Dict, default None
        The...
    """
    def __init__(self,
                 database: Union[Database, str],
                 bulk: bool,
                 reports: bool,
                 cdm: ModuleType,
                 sql_parameters: Optional[Dict] = None):

        if sql_parameters is None:
            sql_parameters = default_sql_parameters
        self.sql_parameters = sql_parameters

        self.db = database
        self.cdm = cdm

        super().__init__(database=self.db, cdm=cdm, bulk=bulk)
        super(OrmWrapper, self).__init__(database=self.db, sql_parameters=self.sql_parameters)

        self.write_reports = reports

        self.etl_stats = EtlStats()

    @property
    def db(self):
        return self._db

    @db.setter
    def db(self, database: Union[Database, str]):
        if isinstance(database, str):
            self._db = Database(database, self.sql_parameters)
        elif isinstance(database, Database):
            self._db = database
        else:
            raise ValueError(f'Unsupported database type: {type(database)}')

    def run(self) -> None:
        print('OMOP wrapper goes brrrrrrrr')

    def load_stcm(self):
        """Insert all stcm csv files into the source_to_concept_map
        table."""
        logger.info('Loading STCM files')
        stcm_dir = Path('./resources/source_to_concept/')
        if not stcm_dir.exists():
            raise FileNotFoundError(f'{str(stcm_dir.resolve())} folder not found')
        stcm_files = stcm_dir.glob('*.csv')
        for stcm_file in stcm_files:
            self.load_source_to_concept_map_from_csv(stcm_file)

    def transform(self) -> None:
        pass

    def stem_table_to_domains(self) -> None:
        """
        Transfer all stem table records to the OMOP clinical data
        tables. Which OMOP table each records is copied into, is
        determined by the target concept's domain_id.
        """
        logger.info('Starting stem table to domain queries')
        post_processing_path = _HERE / 'post_processing'
        self.execute_sql_file(post_processing_path / 'stem_table_to_measurement.sql')
        self.execute_sql_file(post_processing_path / 'stem_table_to_condition_occurrence.sql')
        self.execute_sql_file(post_processing_path / 'stem_table_to_device_exposure.sql')
        self.execute_sql_file(post_processing_path / 'stem_table_to_drug_exposure.sql')
        self.execute_sql_file(post_processing_path / 'stem_table_to_observation.sql')
        self.execute_sql_file(post_processing_path / 'stem_table_to_procedure_occurrence.sql')
        self.execute_sql_file(post_processing_path / 'stem_table_to_specimen.sql')

    def _get_cdm_tables_to_drop(self):
        tables_to_drop = []
        for table in self.db.base.metadata.tables.values():
            default_schema = getattr(table, 'schema', None)
            if default_schema != VOCAB_SCHEMA:
                tables_to_drop.append(table)
        return tables_to_drop

    def drop_cdm(self, tables_to_drop: Optional[List[Table]] = None) -> None:
        """
        Drop non-vocabulary tables defined in the ORM (if they exist).
        :param tables_to_drop: List, default None
            List of SQLAlchemy table definitions that should be dropped.
            If not provided, all tables that by default are not part of
            the CDM vocabulary tables will be dropped.
        :return: None
        """
        logger.info('Dropping OMOP CDM (non-vocabulary) tables if existing')
        if tables_to_drop is None:
            tables_to_drop = self._get_cdm_tables_to_drop()
        conn = self.db.engine.connect()
        conn = conn.execution_options(schema_translate_map=self.db.schema_translate_map)
        self.db.base.metadata.drop_all(bind=conn, tables=tables_to_drop)

    def create_cdm(self) -> None:
        """Create all OMOP CDM tables as defined in base.metadata."""
        logger.info('Creating OMOP CDM (non-vocabulary) tables')
        conn = self.db.engine.connect()
        conn = conn.execution_options(schema_translate_map=self.db.schema_translate_map)
        self.db.base.metadata.create_all(bind=conn)

    def create_schemas_if_not_exists(self) -> None:
        """
        Create the schemas used in the CDM table definitions if they
        don't exist already.
        """
        conn = self.db.engine.connect()
        for schema_name in [
            self.sql_parameters['target_schema'],
            self.sql_parameters['vocab_schema']
        ]:
            if not conn.dialect.has_schema(conn, f'{schema_name}'):
                logger.info(f'Creating schema: {schema_name}')
                self.db.engine.execute(CreateSchema(f'{schema_name}'))
                logger.info(f'Schema created: {schema_name}')
