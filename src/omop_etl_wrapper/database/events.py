from __future__ import annotations

import logging
from collections import Counter
from typing import Iterable, Union

from sqlalchemy import event
from sqlalchemy.orm.persistence import BulkDelete, BulkUpdate
from sqlalchemy.orm.session import Session

from .database import Database
from .session_tracker import SessionTracker
from ..model.etl_stats import EtlTransformation
from ..util.helper import get_full_table_name

logger = logging.getLogger(__name__)


@event.listens_for(Session, "before_flush")
def track_instances_before_flush(session: Session, context, instances):
    if id(session) not in SessionTracker.sessions:
        return
    deletion_counts = Counter(get_record_targets(session.deleted))
    insertion_counts = Counter(get_record_targets(session.new))
    update_counts = Counter(get_record_targets(session.dirty))

    tm: EtlTransformation = SessionTracker.sessions[id(session)]
    tm.deletion_counts += deletion_counts
    tm.insertion_counts += insertion_counts
    tm.update_counts += update_counts


@event.listens_for(Session, 'after_bulk_update')
def receive_after_bulk_update(update_context: BulkUpdate):
    _process_bulk_event(update_context)


@event.listens_for(Session, 'after_bulk_delete')
def receive_after_bulk_delete(delete_context: BulkDelete):
    print(f'Deleted {delete_context.rowcount} {delete_context.primary_table.fullname}')
    _process_bulk_event(delete_context)


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
        placeholder_schema = record.__table__.schema
        schema_name = Database.schema_translate_map.get(placeholder_schema, placeholder_schema)
        table_name = record.__tablename__
        if schema_name is None:
            yield table_name
        else:
            yield '.'.join([schema_name, table_name])


def _process_bulk_event(context: Union[BulkUpdate, BulkDelete]):
    session_id = id(context.session)
    if session_id not in SessionTracker.sessions:
        return

    full_table_name = get_full_table_name(table=context.primary_table.name,
                                          schema=context.primary_table.schema,
                                          schema_map=Database.schema_translate_map)

    tm: EtlTransformation = SessionTracker.sessions[session_id]
    bulk_counts = Counter({full_table_name: context.rowcount})
    if isinstance(context, BulkUpdate):
        tm.update_counts += bulk_counts
    elif isinstance(context, BulkDelete):
        tm.deletion_counts += bulk_counts
