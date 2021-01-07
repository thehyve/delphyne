from typing import Union, Callable

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
