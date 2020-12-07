import logging
from pathlib import Path
from typing import Optional, List

import sys
from sqlalchemy import Table
from sqlalchemy.schema import CreateSchema

from ._paths import STCM_DIR, SOURCE_DATA_CONFIG_PATH
from .cdm import vocabularies as cdm
from .cdm._schema_placeholders import VOCAB_SCHEMA
from .config.models import MainConfig
from .database import Database
from .model.etl_stats import EtlStats
from .model.orm_wrapper import OrmWrapper
from .model.vocab_manager import VocabularyLoader
from .model.raw_sql_wrapper import RawSqlWrapper
from .model.source_data import SourceData
from .util.io import read_yaml_file

logger = logging.getLogger(__name__)

_HERE = Path(__file__).parent


class Wrapper(OrmWrapper, RawSqlWrapper):
    """
    Task coordinator supporting the process of converting source data
    into the OMOP CDM.
    """
    cdm = cdm

    def __init__(self, config: MainConfig, cdm_):
        """
        :param config: MainConfig
            The run configuration as read from config.yml.
        :param cdm_: Module containing the SQLAlchemy declarative Base
            and the CDM tables.
        """
        self.db = Database.from_config(config, cdm_.Base)

        if not self.db.can_connect(str(self.db.engine.url)):
            sys.exit()

        self.write_reports = config.run_options.write_reports

        super().__init__(database=self.db)
        super(OrmWrapper, self).__init__(database=self.db, config=config)

        self.etl_stats = EtlStats()
        self.source_data: Optional[SourceData] = self._set_source_data()
        self.vocab_loader = VocabularyLoader(self.db, cdm_)
        self.load_custom_vocabs: bool = \
            not config.run_options.skip_custom_vocabulary_loading

    def _set_source_data(self):
        if not SOURCE_DATA_CONFIG_PATH.exists():
            logger.info(f'No source data config file found at {SOURCE_DATA_CONFIG_PATH}, '
                        f'assuming no source data files are present')
            return None
        source_config = read_yaml_file(SOURCE_DATA_CONFIG_PATH)
        return SourceData(source_config, self.etl_stats)

    def run(self) -> None:
        print('OMOP wrapper goes brrrrrrrr')

    def load_custom_vocabularies(self):
        logger.info(f'Loading custom vocabulary tables: {self.load_custom_vocabs}')
        if self.load_custom_vocabs:
            self.vocab_loader.load_custom_vocabulary_tables()

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

    def create_schemas(self) -> None:
        """
        Create all schemas used in the SQLAlchemy data model.

        If table definitions include schema names that are present in
        the schema_translate_map, the mapped schema value is used.
        Schemas that already exist remain untouched.
        """
        with self.db.engine.connect() as conn:
            for schema_name in self.db.schemas:
                if not conn.dialect.has_schema(conn, schema_name):
                    logger.info(f'Creating schema: {schema_name}')
                    self.db.engine.execute(CreateSchema(schema_name))
