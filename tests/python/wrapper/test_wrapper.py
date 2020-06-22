import pytest

from src.omop_etl_wrapper.database.database import Database
from src.omop_etl_wrapper.wrapper import Wrapper


@pytest.mark.usefixtures("container")
def test_db_does_not_exist(db_config, caplog):
    uri = f'postgresql://postgres:secret@localhost:{db_config.port}/foobar'
    assert not Database.can_connect(uri)
    assert 'Database "foobar" does not exist' in caplog.text


@pytest.mark.usefixtures("container")
def test_connection_invalid(caplog):
    uri = f'postgresql://postgres:secret@localhost:123456/postgres'
    assert not Database.can_connect(uri)
    assert 'Database "postgres" does not exist' not in caplog.text


@pytest.mark.usefixtures("container")
def test_db_exists(db_config):
    uri = f'postgresql://postgres:secret@localhost:{db_config.port}/postgres'
    assert Database.can_connect(uri)


# @pytest.fixture(scope='module', name='wrapper')
# def get_wrapper_instance():
#     uri = f'postgresql://postgres:secret@localhost:{db_config.port}/etl1'
#     db = Database(uri)
#     source_folder = TEST_DATASETS_DIR / '1000_records'
#     return Wrapper(database=db, source_folder=source_folder,
#                    source_config=SOURCE_DATA_DEFAULT_CONFIG_PATH, bulk=False, reports=False)
#
#
# def test_schema_has_no_tables_before_running(wrapper):
#     assert not wrapper.db.engine.table_names(schema='public')
