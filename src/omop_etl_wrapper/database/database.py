from __future__ import annotations

import logging
from collections import Counter
from contextlib import contextmanager
from getpass import getpass
from typing import Dict, Iterable, Optional, Set

from sqlalchemy import create_engine, event, MetaData
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy_utils.functions import database_exists

from .constraints import ConstraintManager
from .session_tracker import SessionTracker
from ..config.models import MainConfig
from ..model.etl_stats import EtlTransformation

logger = logging.getLogger(__name__)


class Database:
    schema_translate_map: Dict = {}

    def __init__(self, uri: str, schema_translate_map: Dict[str, str], base):
        Database.schema_translate_map = schema_translate_map
        self.engine = create_engine(uri, executemany_mode='values',
                                    execution_options={
                                        "schema_translate_map": schema_translate_map
                                    })
        self.base = base
        self.constraint_manager = ConstraintManager(self)
        self._schemas = self._set_schemas()
        self._sessionmaker = sessionmaker(bind=self.engine, autoflush=False)

    @classmethod
    def from_config(cls, config: MainConfig, base) -> Database:
        """
        Create an instance of Database from a configuration file.

        :param config: MainConfig
            Contents of the configuration file.
        :param base: SQLAlchemy declarative Base
            Base to which the CDM tables are bound via SQLAlchemy's
            declarative model
        :return: Database
        """
        db_config = config.database
        hostname = db_config.host
        port = db_config.port
        database = db_config.database_name
        username = db_config.username
        password = db_config.password.get_secret_value()
        uri = f'postgresql://{username}:{password}@{hostname}:{port}/{database}'
        if not password and Database._password_needed(uri):
            password = getpass('Database password:')
            uri = f'postgresql://{username}:{password}@{hostname}:{port}/{database}'
        return cls(uri=uri, schema_translate_map=config.schema_translate_map, base=base)

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

    @property
    def schemas(self) -> Set[str]:
        return self._schemas.copy()

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

    @property
    def reflected_metadata(self) -> MetaData:
        """
        Get Metadata of the current state of tables in the database.

        :return: SQLAlchemy MetaData
        """
        metadata = MetaData(bind=self.engine)
        for schema in self.schemas:
            metadata.reflect(schema=schema)
        return metadata

    def _set_schemas(self) -> Set[str]:
        schemas: Set[str] = set()
        for table in self.base.metadata.tables.values():
            raw_schema_value = getattr(table, 'schema', None)
            if raw_schema_value is None:
                continue
            if raw_schema_value in self.schema_translate_map:
                schemas.add(self.schema_translate_map[raw_schema_value])
            else:
                schemas.add(raw_schema_value)
        return schemas
