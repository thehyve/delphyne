from __future__ import annotations

import logging
from collections import Counter
from contextlib import contextmanager
from getpass import getpass
from typing import Dict, Iterable, Optional

from sqlalchemy import create_engine, event
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy_utils.functions import database_exists

from .session_tracker import SessionTracker
from ..model.etl_stats import EtlTransformation

logger = logging.getLogger(__name__)

Base = declarative_base()


class Database:
    schema_translate_map: Dict = {}

    def __init__(self, uri: str, schema_translate_map: Dict[str, str]):
        self.engine = create_engine(uri, executemany_mode='values')
        self.base = Base
        self._sessionmaker = sessionmaker(bind=self.engine, autoflush=False)
        Database.schema_translate_map = schema_translate_map

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
    def session_scope(self,
                      raise_on_error: bool = True,
                      metadata: Optional[EtlTransformation] = None
                      ) -> None:
        """
        Provide a transactional scope around a series of operations.

        :param raise_on_error: bool, default True
            if True, raise the exception when the session cannot be
            committed
        :param metadata: EtlTransformation, default None
            if provided, all flushed table mutations (i.e. inserts,
            updates, deletions) will be stored in this object
        :return: None
        """
        session = self.session
        session_id = id(session)
        if metadata is not None:
            SessionTracker.sessions[session_id] = metadata
        session.connection(execution_options={
            "schema_translate_map": self.schema_translate_map
        })
        try:
            yield session
            session.commit()
        except Exception as e:
            logging.error(e, exc_info=True)
            self.perform_rollback(session)
            if metadata is not None:
                metadata.query_success = False
            if raise_on_error:
                raise
        finally:
            SessionTracker.remove_session(session_id)
            session.close()

    @staticmethod
    @event.listens_for(Session, "before_flush")
    def _track_instances_before_flush(session: Session, context, instances):
        if id(session) not in SessionTracker.sessions:
            return
        tm: EtlTransformation = SessionTracker.sessions[id(session)]
        tm.deletion_counts += Counter(Database.get_record_targets(session.deleted))
        tm.insertion_counts += Counter(Database.get_record_targets(session.new))
        tm.update_counts += Counter(Database.get_record_targets(session.dirty))

    @staticmethod
    def get_record_targets(record_containing_object: Iterable) -> Iterable[str]:
        """
        Retrieve target tables of a SQLAlchemy record object. Include
        the target schema if available.

        :param record_containing_object: Iterable
            container of new, updated, or deleted ORM objects
        :return: Iterable[str]
            target table
        """
        for record in record_containing_object:
            placeholder_schema = record.__table_args__.get('schema')
            schema_name = Database.schema_translate_map.get(placeholder_schema, placeholder_schema)
            table_name = record.__tablename__
            if schema_name is None:
                yield table_name
            else:
                yield '.'.join([schema_name, table_name])

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
