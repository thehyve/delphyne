from typing import Union, Callable, Optional, Dict

from sqlalchemy import Table

from ..database import Database


def table_is_empty(mapped_table: Union[Callable, Table], database: Database) -> bool:
    """
    Check whether a database table contains no records.

    :param mapped_table: Union[Callable, Table]
        A declarative SQLAlchemy table class or Table instance
    :param database: Database
        Database instance
    :return: bool
        Return True if the table is empty
    """
    with database.session_scope() as session:
        return session.query(mapped_table).first() is None


def get_full_table_name(table: str,
                        schema: Optional[str],
                        schema_map: Optional[Dict[str, str]] = None
                        ) -> str:
    """
    '.' join schema and table name to get the full table name.

    If schema is not available, return only the table name. Placeholder
    schema names will be replaced according to the schema_map.

    :param table: str
    :param schema: str
    :param schema_map: Dict[str, str], default None
        Schema map dictionary with placeholder names as keys and actual
        schema names as values.
    :return: str
    """
    if schema is None:
        return table
    if schema_map:
        schema = schema_map.get(schema, schema)
    return '.'.join([schema, table])
