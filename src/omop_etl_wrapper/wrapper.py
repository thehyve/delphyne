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
import sys
from pathlib import Path
from typing import Optional, Dict, List, Set

from sqlalchemy import Table
from sqlalchemy.schema import CreateSchema

from ._paths import STCM_DIR
from .cdm._schema_placeholders import VOCAB_SCHEMA
from .database import Database
from .model.etl_stats import EtlStats
from .model.orm_wrapper import OrmWrapper
from .model.vocabulary_loader import VocabularyLoader
from .model.raw_sql_wrapper import RawSqlWrapper

logger = logging.getLogger(__name__)

_HERE = Path(__file__).parent


class Wrapper(OrmWrapper, RawSqlWrapper):
    """
    Task coordinator supporting the process of converting source data
    into the OMOP CDM.

    config : Dict
        The run configuration as read from config.yml.
    """
    def __init__(self, config: Dict[str, Dict]):
        self.db = Database.from_config(config)
        self.bulk_mode = config['run_options']['bulk_mode']
        self.write_reports = config['run_options']['write_reports']
        self._cdm = self._set_cdm_version(config['run_options']['cdm'])

        if not self.db.can_connect(str(self.db.engine.url)):
            sys.exit()

        super().__init__(database=self.db, cdm=self._cdm, bulk=self.bulk_mode)
        super(OrmWrapper, self).__init__(database=self.db, config=config)

        self.etl_stats = EtlStats()
        self.vocab_loader = VocabularyLoader(self.db, self._cdm)

    @staticmethod
    def _set_cdm_version(cdm: str):
        if cdm.upper() == 'CDM531':
            from .cdm import cdm531 as orm
        elif cdm.upper() == 'CDM600':
            from .cdm import cdm600 as orm
        elif cdm.upper() == 'HYBRID':
            from .cdm import hybrid as orm
        else:
            raise ValueError(f'Unrecognized CDM version: "{cdm}"')
        return orm

    def run(self) -> None:
        print('OMOP wrapper goes brrrrrrrr')

    def load_stcm(self):
        """Insert all stcm csv files into the source_to_concept_map
        table."""
        logger.info('Loading STCM files')
        if not STCM_DIR.exists():
            raise FileNotFoundError(f'{str(STCM_DIR.resolve())} folder not found')
        # TODO: support multiple file extensions
        stcm_files = STCM_DIR.glob('*.csv')
        for stcm_file in stcm_files:
            self.load_source_to_concept_map_from_csv(stcm_file)

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
            placeholder_schema = getattr(table, 'schema', None)
            table_schema = self.db.schema_translate_map.get(placeholder_schema)
            if table_schema != self.db.schema_translate_map[VOCAB_SCHEMA]:
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

    def _get_schemas_to_create(self) -> Set[str]:
        schemas: Set[str] = set()
        for table in self.db.base.metadata.tables.values():
            placeholder_schema = getattr(table, 'schema', None)
            if not placeholder_schema:
                continue
            if placeholder_schema in self.db.schema_translate_map:
                schemas.add(self.db.schema_translate_map[placeholder_schema])
            else:
                schemas.add(placeholder_schema)
        return schemas

    def create_schemas(self) -> None:
        """
        Create the schemas as present in the schema_translate_map of
        the config file and ORM table definitions (if they don't exist
        already).
        """
        conn = self.db.engine.connect()
        schemas = self._get_schemas_to_create()
        for schema_name in schemas:
            if not conn.dialect.has_schema(conn, f'{schema_name}'):
                logger.info(f'Creating schema: {schema_name}')
                self.db.engine.execute(CreateSchema(f'{schema_name}'))
                logger.info(f'Schema created: {schema_name}')
