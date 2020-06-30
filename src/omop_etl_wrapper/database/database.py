from __future__ import annotations

import logging
from contextlib import contextmanager
from getpass import getpass
from typing import Dict

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy_utils.functions import database_exists

logger = logging.getLogger(__name__)

Base = declarative_base()


class Database:
    def __init__(self, uri: str, schema_translate_map: Dict[str, str]):
        self.engine = create_engine(uri, executemany_mode='values')
        self.base = Base
        self.schema_translate_map = schema_translate_map
        self._sessionmaker = sessionmaker(bind=self.engine, autoflush=False)

    @classmethod
    def from_config(cls, config: Dict) -> Database:
        """
        Create an instance of Database from a configuration file.

        :param config: Dict
            Contents of the configuration file.
        :return: Database
        """
        db_config = config['database']
        hostname = db_config['host']
        port = db_config['port']
        database = db_config['database_name']
        username = db_config['username']
        password = db_config['password']
        uri = f'postgresql://{username}:{password}@{hostname}:{port}/{database}'
        if not password and Database._password_needed(uri):
            password = getpass('Database password:')
            uri = f'postgresql://{username}:{password}@{hostname}:{port}/{database}'
        return cls(uri=uri, schema_translate_map=config['schema_translate_map'])

    @staticmethod
    def _password_needed(uri: str) -> bool:
        logger.disabled = True
        try:
            create_engine(uri).connect()
        except OperationalError as e:
            if 'no password supplied' in str(e):
                return True
        else:
            return False
        finally:
            logger.disabled = False

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
        """
        Provide a transactional scope around a series of operations.
        """
        session = self.session
        session.connection(execution_options={
            "schema_translate_map": self.schema_translate_map
        })
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
