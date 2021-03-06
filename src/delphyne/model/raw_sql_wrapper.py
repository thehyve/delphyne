"""Raw SQL query module."""

import logging
import re
from collections import Counter
from pathlib import Path
from typing import Callable, Dict, Union, Optional

from sqlalchemy import text, Table, MetaData
from sqlalchemy.engine.result import ResultProxy
from sqlalchemy.exc import InvalidRequestError

from .etl_stats import EtlTransformation, open_transformation
from .._paths import SQL_TRANSFORMATIONS_DIR
from ..config.models import MainConfig
from ..database.database import Database
from ..util.helper import replace_substrings

logger = logging.getLogger(__name__)


class RawSqlWrapper:
    """
    Wrapper which coordinates the execution of raw SQL transformations.

    Parameters
    ----------
    database : Database
        Database instance to interact with.
    config : MainConfig
        Main configuration contents.
    """

    def __init__(self, database: Database, config: MainConfig):
        self.db = database
        self.sql_parameters = self._get_sql_parameters(config)

    @staticmethod
    def _get_sql_parameters(config: MainConfig):
        sql_parameters = config.sql_parameters
        if sql_parameters is None:
            return config.schema_translate_map
        # Add schema maps to sql_parameters, unless already present
        for k, v in config.schema_translate_map.items():
            if k not in sql_parameters:
                sql_parameters[k] = v
        return sql_parameters

    def execute_sql_file(self, file_path: Union[Path, str]) -> None:
        """
        Execute a raw SQL query from a file.

        Parameters
        ----------
        file_path : pathlib.Path or str
            Relative SQL file path inside the directory for SQL
            transformations (the root will be automatically added).

        Returns
        -------
        None
        """
        file_path = SQL_TRANSFORMATIONS_DIR / file_path
        # Open and read the file as a single buffer
        logger.debug(f'Reading query from file: {file_path.name}')
        with file_path.open('r') as f:
            query = f.read().strip()

        self.execute_sql_query(query=query, query_name=file_path.name)

    def execute_sql_query(self, query: str, query_name: str) -> None:
        """
        Execute a raw SQL query.

        Parameters
        ----------
        query : str
            Full SQL query as string.
        query_name : str
            Name of the transformation.

        Returns
        -------
        None
        """
        logger.info(f'Executing raw sql query: {query_name}')
        with open_transformation(name=query_name) as transformation_metadata:
            query = self.apply_sql_parameters(query, self.sql_parameters)

            with self.db.engine.connect() as con:
                try:
                    statement = text(query).execution_options(autocommit=True)
                    result = con.execute(statement)
                    self._collect_query_statistics(result, query, transformation_metadata)
                except Exception as msg:
                    logger.error(f'Query failed: {query_name}')
                    logger.error(query)
                    logger.error(msg)
                    transformation_metadata.query_success = False

    def execute_sql_transformation(self, statement: Callable) -> None:
        """
        Execute an ETL transformation via a python statement.

        Recommended for table to table transformations, i.e. when the
        source data is read from an existing database table instead of
        file. The statement must return a SQLAlchemy query object.

        Parameters
        ----------
        statement : Callable
            Python function which takes this wrapper as input and
            returns a SQLAlchemy query object to be executed.
            It will be called as a transformation.

        Returns
        -------
        None
        """
        logger.info(f'Executing transformation: {statement.__name__}')
        with self.db.tracked_session_scope(name=statement.__name__, raise_on_error=False) \
                as (session, transformation_metadata):
            query = statement(self)
            result = session.execute(query)
            query_string = result.context.statement
            self._collect_query_statistics(result, query_string, transformation_metadata)

    def get_table(self, schema: str, table_name: str) -> Optional[Table]:
        """
        Get a SQLAlchemy Table object from an existing database schema.

        The table object is obtained through metadata reflection; useful
        when retrieving tables that are not defined in the ORM model
        (e.g. table is in a custom source schema). For tables that
        already have an ORM definition, this method is equivalent to
        using <TableName>.__table__.

        Parameters
        ----------
        schema: str
            Name of the schema to retrieve the table from. This may
            also be a schema placeholder, in which case it will be
            translated to the runtime schema name.
        table_name : str
            Name of the table to retrieve. An error message will be
            displayed if the table cannot be found.

        Returns
        -------
        Table
        """
        m = MetaData(bind=self.db.engine)
        schema = self.db.schema_translate_map.get(schema, schema)
        try:
            m.reflect(schema=schema, only=[table_name], resolve_fks=False)
        except InvalidRequestError as e:
            logger.error(f'Table with name "{table_name}" not found in "{schema}" schema')
            raise e
        return m.tables[f'{schema}.{table_name}']

    @staticmethod
    def apply_sql_parameters(parameterized_query: str, sql_parameters: Dict[str, str]) -> str:
        """
        Create finalized SQL query by replacing any parameters.

        Parameters
        ----------
        parameterized_query : str
            SQL query optionally containing placeholders as indicated
            by an '@'.
        sql_parameters : dict of {str : str}
            Placeholder to final value mapping.

        Returns
        -------
        str
            The finalized SQL query.
        """
        replacement_dict = {'@' + key: value for key, value in sql_parameters.items()}
        return replace_substrings(parameterized_query, replacement_dict)

    def _collect_query_statistics(self,
                                  result: ResultProxy,
                                  query: str,
                                  transformation_metadata: EtlTransformation
                                  ) -> None:
        query_type = self._parse_query_type(query)
        target_table: str = self._parse_target_table_from_query(query)
        row_count: int = result.rowcount

        logger.info(f'Saved {row_count} objects')

        if query_type == 'INSERT':
            transformation_metadata.insertion_counts = Counter({target_table: row_count})
        elif query_type == 'UPDATE':
            transformation_metadata.update_counts = Counter({target_table: row_count})
        elif query_type == 'DELETE':
            transformation_metadata.deletion_counts = Counter({target_table: row_count})

    @staticmethod
    def _parse_query_type(query: str) -> Optional[str]:
        # Find the query type of the provided query, indicating whether
        # it was an insert, update or delete statement.
        match = RawSqlWrapper._parse_raw_sql_query(query)
        if match is None:
            return None

        statement = match.group(1).upper()
        if 'INTO' in statement or 'CREATE TABLE' in statement:
            return 'INSERT'
        elif 'DELETE' in statement:
            return 'DELETE'
        elif 'UPDATE' in statement:
            return 'UPDATE'
        else:
            return None

    @staticmethod
    def _parse_target_table_from_query(query: str) -> str:
        # Find the target table of the provided query.
        # If not found return '?'.
        match = RawSqlWrapper._parse_raw_sql_query(query)
        if match:
            return match.group(2).lower()
        else:
            return '?'

    @staticmethod
    def _parse_raw_sql_query(query: str) -> Optional[re.Match]:
        # Regex search a raw sql query to find query_type and target
        match = re.search(
            r'^\s*((?:INSERT )?INTO|CREATE TABLE|DELETE\s+FROM|UPDATE)\s+(.+?)\s',
            query,
            re.IGNORECASE | re.MULTILINE
        )
        return match
