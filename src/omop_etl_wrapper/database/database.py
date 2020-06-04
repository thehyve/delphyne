import logging
from contextlib import contextmanager
from typing import Optional, Dict

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy_utils.functions import database_exists

base = declarative_base()

logger = logging.getLogger(__name__)

# TODO: make default schemas "vocab" and "cdm" single reference
#  variables for all files that use them

# TODO: Deduplicate with the params from wrapper.py
_default_sql_parameters = {
    'vocab_schema': 'vocab',
    'source_schema': 'source',
    'target_schema': 'cdm_palermo'
}


class Database:
    def __init__(self, uri: str, sql_parameters: Optional[Dict[str, str]]):
        self.engine = create_engine(uri, executemany_mode='values')
        self.base = base
        if sql_parameters is not None:
            self.sql_parameters = sql_parameters
        else:
            self.sql_parameters = _default_sql_parameters

        self.schema_translate_map: Dict[str, str] = self._set_schema_map()
        self._sessionmaker = sessionmaker(bind=self.engine, autoflush=False)

    def _set_schema_map(self):
        schema_map = {
            'vocab': self.sql_parameters.get('vocab_schema', 'vocab'),
            'cdm': self.sql_parameters.get('target_schema', 'cdm'),
        }
        return schema_map

    @property
    def session(self) -> Session:
        logger.debug('Creating new session')
        return self._sessionmaker()

    def close_connection(self) -> None:
        self.engine.dispose()

    @staticmethod
    def perform_rollback(session: Session) -> None:
        logger.info('Performing rollback')
        session.rollback()
        logger.info('Rollback completed')

    @contextmanager
    def session_scope(self) -> None:
        """Provide a transactional scope around a series of operations."""
        session = self.session
        session.connection(execution_options={
            "schema_translate_map": self.schema_translate_map})
        try:
            yield session
            session.commit()
        except Exception as e:
            logger.error(e)
            self.perform_rollback(session)
            raise
        finally:
            session.close()

    @staticmethod
    def can_connect(uri: str) -> bool:
        try:
            db_exists = database_exists(uri)
        except OperationalError as e:
            logger.error(e)
            return False
        if not db_exists:
            db_name = uri.rsplit('/', 1)[-1]
            logger.error(f'Could not connect. Database "{db_name}" does not exist')
        return db_exists