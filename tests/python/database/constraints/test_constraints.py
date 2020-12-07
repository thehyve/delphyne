import logging
from typing import Set

import pytest
from sqlalchemy import Table, Index, Constraint, MetaData
from src.omop_etl_wrapper import Wrapper

from tests.python.conftest import docker_not_available
from tests.python.database.constraints import constraint_sets as expected_sets

pytestmark = pytest.mark.skipif(condition=docker_not_available(),
                                reason='Docker daemon is not running')


def reflect_table(wrapper: Wrapper, full_table_name: str) -> Table:
    metadata = MetaData(bind=wrapper.db.engine)
    table_without_schema = full_table_name.split('.')[-1]
    metadata.reflect(schema='cdm', only=[table_without_schema])
    return metadata.tables[full_table_name]


def get_index_names(indexes: Set[Index]) -> Set[str]:
    return {i.name for i in indexes}


def get_constraint_names(constraints: Set[Constraint]) -> Set[str]:
    return {c.name for c in constraints}


def get_single_table_object_names(table: Table) -> Set[str]:
    """Get the names of all constraints/indexes present on the table."""
    index_names = get_index_names(table.indexes)
    constraint_names = get_constraint_names(table.constraints)
    return index_names.union(constraint_names)


def get_all_db_table_object_names(metadata: MetaData) -> Set[str]:
    """Get the names of all constraints/indexes of all tables."""
    all_db_objects = set()
    for table in metadata.tables.values():
        all_db_objects = all_db_objects.union(get_index_names(table.indexes))
        all_db_objects = all_db_objects.union(get_constraint_names(table.constraints))
    return all_db_objects


@pytest.mark.usefixtures("container", "test_db")
def test_drop_and_add_single_index(cdm531_wrapper_with_tables_created: Wrapper):
    person_id_index = 'ix_measurement_person_id'
    full_table_name = 'cdm.measurement'
    wrapper = cdm531_wrapper_with_tables_created

    # All expected indexes on the measurement table are there initially
    meas_table = reflect_table(wrapper, full_table_name)
    expected_full = expected_sets.measurement_indexes_full
    assert get_index_names(meas_table.indexes) == expected_full

    # Calling drop_index will remove the index on person_id
    wrapper.db.constraint_manager.drop_index(person_id_index)
    meas_table = reflect_table(wrapper, full_table_name)
    expected = expected_sets.measurement_indexes_without_person_index
    assert get_index_names(meas_table.indexes) == expected

    # Calling add_index will restore the index on person_id
    wrapper.db.constraint_manager.add_index(person_id_index)
    meas_table = reflect_table(wrapper, full_table_name)
    assert get_index_names(meas_table.indexes) == expected_full


@pytest.mark.usefixtures("container", "test_db")
def test_drop_and_add_pk(cdm600_wrapper_with_tables_created: Wrapper):
    survey_conduct_pk = 'pk_survey_conduct'
    full_table_name = 'cdm.survey_conduct'
    wrapper = cdm600_wrapper_with_tables_created

    sc_table = reflect_table(wrapper, full_table_name)
    assert sc_table.primary_key.name == 'pk_survey_conduct'

    wrapper.db.constraint_manager.drop_constraint(survey_conduct_pk)
    sc_table = reflect_table(wrapper, full_table_name)
    # SQLAlchemy leaves an empty PrimaryKeyConstraint object
    assert sc_table.primary_key.name is None

    wrapper.db.constraint_manager.add_constraint(survey_conduct_pk)
    sc_table = reflect_table(wrapper, full_table_name)
    assert sc_table.primary_key.name == 'pk_survey_conduct'


@pytest.mark.usefixtures("container", "test_db")
def test_drop_index_raises_keyerror_if_missing(cdm600_wrapper_with_tables_created: Wrapper):
    wrapper = cdm600_wrapper_with_tables_created

    # Cannot drop index that never existed
    with pytest.raises(KeyError):
        wrapper.db.constraint_manager.drop_index('index_404')

    # This index can be dropped
    wrapper.db.constraint_manager.drop_index('ix_measurement_person_id')
    # But not again once already dropped
    with pytest.raises(KeyError):
        wrapper.db.constraint_manager.drop_index('ix_measurement_person_id')


@pytest.mark.usefixtures("container", "test_db")
def test_diff_index_name_is_recognized(cdm600_wrapper_with_tables_created: Wrapper, caplog):
    wrapper = cdm600_wrapper_with_tables_created
    full_table_name = 'cdm.specimen'

    # Rename index ix_specimen_person_id
    rename_query = "ALTER INDEX cdm.ix_specimen_person_id RENAME TO indexus_anderus;"
    with wrapper.db.engine.connect() as conn:
        conn.execute(rename_query)

    # Verify it has a new name
    specimen_table = reflect_table(wrapper, full_table_name)
    indexes = get_index_names(specimen_table.indexes)
    assert indexes == {'ix_specimen_specimen_concept_id', 'indexus_anderus'}

    # Trying to add the original index will have no effect
    with caplog.at_level(logging.INFO):
        wrapper.db.constraint_manager.add_index('ix_specimen_person_id')
    specimen_table = reflect_table(wrapper, full_table_name)
    indexes = get_index_names(specimen_table.indexes)
    assert indexes == {'ix_specimen_specimen_concept_id', 'indexus_anderus'}
    assert 'equivalent already exists with name "indexus_anderus"' in caplog.text


@pytest.mark.usefixtures("container", "test_db")
def test_drop_and_add_table_constraints(cdm600_wrapper_with_tables_created: Wrapper):
    full_table_name = 'cdm.specimen'
    wrapper = cdm600_wrapper_with_tables_created

    # Initially all constraints/indexes are present on the table
    table = reflect_table(wrapper, full_table_name)
    all_names = get_single_table_object_names(table)
    assert all_names == expected_sets.all_specimen_objects

    # Calling drop_table_constraints without arguments drops everything
    wrapper.db.constraint_manager.drop_table_constraints('specimen')
    table = reflect_table(wrapper, full_table_name)
    all_names = get_single_table_object_names(table)
    assert all_names == {None}  # Only PK husk remains

    # Calling add_table_constraints restores all constraints/indexes
    wrapper.db.constraint_manager.add_table_constraints('specimen')
    table = reflect_table(wrapper, full_table_name)
    all_names = get_single_table_object_names(table)
    assert all_names == expected_sets.all_specimen_objects


@pytest.mark.usefixtures("container", "test_db")
def test_drop_and_add_cdm_constraints(cdm600_wrapper_with_tables_created: Wrapper):
    wrapper = cdm600_wrapper_with_tables_created

    # Initially all constraints/indexes are present
    all_db_objects = get_all_db_table_object_names(wrapper.db.reflected_metadata)
    assert all_db_objects == expected_sets.db_table_objects_full

    # Calling drop_cdm_constraints without arguments drops everything on
    # non-vocab tables
    wrapper.db.constraint_manager.drop_cdm_constraints()
    all_db_objects = get_all_db_table_object_names(wrapper.db.reflected_metadata)
    assert all_db_objects == {None}.union(expected_sets.vocab_table_objects)

    # Calling add_cdm_constraints restores all constraints/indexes
    wrapper.db.constraint_manager.add_cdm_constraints()
    all_db_objects = get_all_db_table_object_names(wrapper.db.reflected_metadata)
    assert all_db_objects == expected_sets.db_table_objects_full
