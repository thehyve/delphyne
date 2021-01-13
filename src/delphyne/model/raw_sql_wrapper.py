import logging
import re
from collections import Counter
from pathlib import Path
from typing import Dict, Union

from sqlalchemy import text
from sqlalchemy.engine.result import ResultProxy

from .etl_stats import EtlTransformation, open_transformation
from .._paths import SQL_TRANSFORMATIONS_DIR
from ..config.models import MainConfig
from ..database.database import Database
from ..util.helper import replace_substrings

logger = logging.getLogger(__name__)


class RawSqlWrapper:
    """
    Wrapper which coordinates the execution of raw SQL transformations.
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
        Executes raw SQL file.

        :param file_path: relative SQL file path inside the directory
            for SQL transformations
            (the root will be automatically added).
        :return: None
        """
        file_path = SQL_TRANSFORMATIONS_DIR / file_path
        # Open and read the file as a single buffer
        logger.debug(f'Reading query from file: {file_path.name}')
        with file_path.open('r') as f:
            query = f.read().strip()

        self.execute_sql_query(query=query, query_name=file_path.name)

    def execute_sql_query(self, query: str, query_name: str) -> None:
        logger.info(f'Executing raw sql query: {query_name}')
        with open_transformation(name=query_name) as transformation_metadata:
            query = self.apply_sql_parameters(query, self.sql_parameters)

            with self.db.engine.connect() as con:
                try:
                    statement = text(query).execution_options(autocommit=True)
                    result = con.execute(statement)
                    self._collect_transformation_statistics(result, query, transformation_metadata)
                except Exception as msg:
                    logger.error(f'Query failed: {query_name}')
                    logger.error(query)
                    logger.error(msg)
                    transformation_metadata.query_success = False

    @staticmethod
    def apply_sql_parameters(parameterized_query: str, sql_parameters: Dict[str, str]):
        """
        Create finalized SQL query by replacing the parameters.

        :param parameterized_query: str
            Query containing parameters indicated by a '@'
        :param sql_parameters: Dict[str, str]
            Parameter name to value mapping
        :return: str
        """
        replacement_dict = {'@' + key: value for key, value in sql_parameters.items()}
        return replace_substrings(parameterized_query, replacement_dict)

    def _collect_transformation_statistics(self,
                                           result: ResultProxy,
                                           query: str,
                                           transformation_metadata: EtlTransformation
                                           ) -> None:
        status_message: str = result.context.cursor.statusmessage
        target_table: str = self.parse_target_table_sqlquery(query)
        row_count: int = result.rowcount

        if status_message.startswith('INSERT'):
            transformation_metadata.insertion_counts = Counter({target_table: row_count})
        elif status_message.startswith('UPDATE'):
            transformation_metadata.update_counts = Counter({target_table: row_count})
        elif status_message.startswith('DELETE'):
            transformation_metadata.deletion_counts = Counter({target_table: row_count})

    @staticmethod
    def parse_target_table_sqlquery(query: str) -> str:
        """
        Find the target table of the provided query.

        :param query: str
            The SQL query to be parsed for a target table
        :return target table: str
            The target table as present in the query. If not found
            return '?'
        """
        match = re.search(r'^\s*((?:INSERT )?INTO|CREATE TABLE|DELETE\s+FROM|UPDATE)\s+(.+?)\s',
                          query,
                          re.IGNORECASE | re.MULTILINE
                          )
        if match:
            return match.group(2).lower()
        else:
            return '?'
