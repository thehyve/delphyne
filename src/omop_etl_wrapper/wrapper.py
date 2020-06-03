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
from typing import Optional, Union, Dict

from sqlalchemy.schema import CreateSchema

from .cdm import hybrid  # TODO: make cdm version agnostic
from .database.database import Database
from .model.etl_stats import EtlStats
from .model.orm_wrapper import OrmWrapper
from .model.raw_sql_wrapper import RawSqlWrapper


logger = logging.getLogger(__name__)

_HERE = Path(__file__)

_default_sql_parameters = {
    'vocab_schema': 'vocab',
    'source_schema': 'source',
    'target_schema': 'cdm_palermo'
}


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
    sql_parameters : Dict, default None
        The...
    """
    def __init__(self,
                 database: Union[Database, str],
                 bulk: bool,
                 reports: bool,
                 sql_parameters: Optional[Dict] = None):

        if sql_parameters is None:
            sql_parameters = _default_sql_parameters
        self.sql_parameters = sql_parameters

        self.db = database

        super().__init__(database=self.db, bulk=bulk)
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
        pass
        # TODO: create separate method for clearing the STCM table, then
        #  make this method loop through all csv files present and
        #  insert the contents
        # self.load_source_to_concept_map_from_csv(STCM_DIR / 'AE_source_to_concept.csv', truncate_first=True)

    def transform(self) -> None:
        pass

    def stem_table_to_domains(self) -> None:
        logger.info('Starting stem table to domain queries')
        post_processing_path = _HERE / 'post_processing'
        self.execute_sql_file(post_processing_path / 'stem_table_to_measurement.sql')
        self.execute_sql_file(post_processing_path / 'stem_table_to_condition_occurrence.sql')
        self.execute_sql_file(post_processing_path / 'stem_table_to_device_exposure.sql')
        self.execute_sql_file(post_processing_path / 'stem_table_to_drug_exposure.sql')
        self.execute_sql_file(post_processing_path / 'stem_table_to_observation.sql')
        self.execute_sql_file(post_processing_path / 'stem_table_to_procedure_occurrence.sql')
        self.execute_sql_file(post_processing_path / 'stem_table_to_specimen.sql')

    def drop_cdm(self) -> None:
        """Drop clinical tables, if they exist."""
        logger.info('Dropping OMOP CDM (non-vocabulary) tables if existing')
        tables = []
        # TODO: needs a less hacky implementation
        for name, cls in hybrid.__dict__.items():
            if isinstance(cls, type):
                file_module = cls.__module__.rsplit('.', 1)[-1]
                if file_module != 'vocabularies':
                    tables.append(cls.__table__)
        conn = self.db.engine.connect()
        conn = conn.execution_options(schema_translate_map=self.db.schema_translate_map)
        self.db.base.metadata.drop_all(bind=conn, tables=tables)

    def create_cdm(self) -> None:
        """Create all OMOP CDM tables as defined in base.metadata."""
        logger.info('Creating OMOP CDM (non-vocabulary) tables')
        conn = self.db.engine.connect()
        conn = conn.execution_options(schema_translate_map=self.db.schema_translate_map)
        self.db.base.metadata.create_all(bind=conn)

    def create_schemas_if_not_exists(self) -> None:
        """Create the schemas used in the CDM table definitions if they
        don't exist already."""
        conn = self.db.engine.connect()
        for schema_name in [
            self.sql_parameters['target_schema'],
            self.sql_parameters['vocab_schema']
        ]:
            if not conn.dialect.has_schema(conn, f'{schema_name}'):
                logger.info(f'Creating schema: {schema_name}')
                self.db.engine.execute(CreateSchema(f'{schema_name}'))
                logger.info(f'Schema created: {schema_name}')
