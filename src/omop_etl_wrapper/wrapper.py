import logging
from pathlib import Path
from typing import Optional, List

import sys
from sqlalchemy import Table
from sqlalchemy.schema import CreateSchema

from ._paths import SOURCE_DATA_CONFIG_PATH
from .cdm import vocabularies as cdm
from .cdm._schema_placeholders import VOCAB_SCHEMA
from .config.models import MainConfig
from .database import Database
from .model.etl_stats import etl_stats
from .model.mapping import CodeMapper
from .model.orm_wrapper import OrmWrapper
from .model.raw_sql_wrapper import RawSqlWrapper
from .model.source_data import SourceData
from .model.vocab_manager import VocabManager
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
        etl_stats.reset()
        self._config = config
        self.db = Database.from_config(config, cdm_.Base)

        if not self.db.can_connect(str(self.db.engine.url)):
            sys.exit()

        super().__init__(database=self.db)
        super(OrmWrapper, self).__init__(database=self.db, config=config)

        self.source_data: Optional[SourceData] = self._set_source_data()
        self.vocab_manager = VocabManager(self.db, cdm_, config)
        self.code_mapper = CodeMapper(self.db, cdm_)

    def _set_source_data(self):
        source_data_path = self._config.source_data_folder
        if source_data_path is None:
            logger.info(f'No source_data_folder provided in config file, '
                        f'assuming no source data files are present')
            return None
        if not SOURCE_DATA_CONFIG_PATH.exists():
            logger.info(f'No source data config file found at {SOURCE_DATA_CONFIG_PATH}, '
                        f'assuming no source data files are present')
            return None
        source_config = read_yaml_file(SOURCE_DATA_CONFIG_PATH)
        source_config['source_data_folder'] = source_data_path
        return SourceData(source_config)

    def run(self) -> None:
        print('OMOP wrapper goes brrrrrrrr')

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
        with self.db.engine.connect() as conn:
            self.db.base.metadata.drop_all(bind=conn, tables=tables_to_drop)

    def create_cdm(self) -> None:
        """Create all OMOP CDM tables as defined in base.metadata."""
        logger.info('Creating OMOP CDM (non-vocabulary) tables')
        with self.db.engine.connect() as conn:
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

    def summarize(self) -> None:
        etl_stats.log_summary()
        if self._config.run_options.write_reports:
            etl_stats.write_summary_files()
