"""Database operations module."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from getpass import getpass
from types import MappingProxyType
from typing import Dict, Set, FrozenSet, ContextManager, Tuple

from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy_utils.functions import database_exists

from .constraints import ConstraintManager
from .session_tracker import SessionTracker
from ..config.models import MainConfig
from ..model.etl_stats import EtlTransformation, open_transformation

logger = logging.getLogger(__name__)


class Database:
    """
    Handler for all interactions with the database.

    Parameters
    ----------
    uri : str
        Database URI for creating the SQLAlchemy engine.
    schema_translate_map: dict of {str : str}
        Contains the schema placeholder to actual schema name mappings.
    base : SQLAlchemy declarative base
        SQLAlchemy declarative base to which all CDM tables are bound.

    Attributes
    ----------
    schemas
    reflected_metadata
    engine : sqlalchemy.engine.base.Engine
        Database engine.
    constraint_manager : ConstraintManager
        Access point to alter constraints/indexes of the database.
    """

    schema_translate_map: MappingProxyType = None

    def __init__(self, uri: str, schema_translate_map: Dict[str, str], base):
        Database.schema_translate_map = MappingProxyType(schema_translate_map)
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

        Parameters
        ----------
        config : MainConfig
            Contents of the configuration file.
        base : SQLAlchemy declarative Base
            Base to which the CDM tables are bound via SQLAlchemy's
            declarative model

        Returns
        -------
        Database
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
    def schemas(self) -> FrozenSet[str]:
        """Database schemas used in CDM."""
        return self._schemas

    def get_new_session(self) -> Session:
        """
        Get a new database session.

        Returns
        -------
        Session
            SQLAlchemy open session.
        """
        logger.debug('Creating new session')
        return self._sessionmaker()

    def close_connection(self) -> None:
        """
        Dispose database engine.

        Returns
        -------
        None
        """
        self.engine.dispose()

    @staticmethod
    def _perform_rollback(session: Session) -> None:
        logger.info('Performing rollback')
        session.rollback()
        logger.info('Rollback completed')

    @contextmanager
    def session_scope(self,
                      raise_on_error: bool = True,
                      ) -> ContextManager[Session]:
        """
        Provide a transactional scope.

        Before closing, the session will try to commit any changes that
        were made. If the commit fails, a rollback is performed.

        Parameters
        ----------
        raise_on_error : bool, default True
            If False, when the session cannot be committed, close
            session and return. Otherwise raise the exception.

        Yields
        ------
        Session
            SQLAlchemy open session.
        """
        session = self.get_new_session()
        try:
            yield session
            session.commit()
        except Exception as e:
            logging.error(e, exc_info=True)
            self._perform_rollback(session)
            if raise_on_error:
                raise
        finally:
            session.close()

    @contextmanager
    def tracked_session_scope(self,
                              name: str,
                              raise_on_error: bool = True,
                              ) -> ContextManager[Tuple[Session, EtlTransformation]]:
        """
        Provide a transactional scope, tracking table record changes.

        Before closing, the session will try to commit any changes that
        were made. If the commit fails, a rollback is performed.
        Changes made in the session will be captured in an
        EtlTransformation instance that will be added to etl_stats.

        Parameters
        ----------
        name : str
            Name of the transformation as captured in an
            EtlTransformation metadata instance.
        raise_on_error : bool, default True
            If False, when the session cannot be committed, close
            session and return. Otherwise raise the exception.

        Yields
        ------
        Session
            SQLAlchemy open session.
        EtlTransformation
            Container storing metadata about the session.
        """
        session = self.get_new_session()
        session_id = id(session)
        with open_transformation(name=name) as metadata:
            SessionTracker.sessions[session_id] = metadata
            try:
                yield session, metadata
                session.commit()
            except Exception as e:
                logging.error(e, exc_info=True)
                self._perform_rollback(session)
                metadata.query_success = False
                if raise_on_error:
                    raise
            finally:
                SessionTracker.remove_session(session_id)
                session.close()

    @staticmethod
    def can_connect(uri: str) -> bool:
        """
        Check whether a connection can be established for the given URI.

        Parameters
        ----------
        uri : str
            URI including database name.

        Returns
        -------
        bool
            Returns True if connection to database could be established.
        """
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
        """Metadata of the current state of tables in the database."""
        metadata = MetaData(bind=self.engine)
        for schema in self.schemas:
            metadata.reflect(schema=schema)
        return metadata

    def _set_schemas(self) -> FrozenSet[str]:
        schemas: Set[str] = set()
        for table in self.base.metadata.tables.values():
            raw_schema_value = getattr(table, 'schema', None)
            if raw_schema_value is None:
                continue
            if raw_schema_value in self.schema_translate_map:
                schemas.add(self.schema_translate_map[raw_schema_value])
            else:
                schemas.add(raw_schema_value)
        return frozenset(schemas)
