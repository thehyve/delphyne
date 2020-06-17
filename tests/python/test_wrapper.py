import os
from collections import namedtuple
from pathlib import Path
from time import sleep

import docker
import psycopg2
import pytest

from src.omop_etl_wrapper.database.database import Database
from src.omop_etl_wrapper.wrapper import Wrapper

_HERE = Path(__file__).parent

DbConfig = namedtuple('DbConfig', ['host', 'dbname', 'user', 'password', 'port'])

db_config = DbConfig(
    host='localhost',
    dbname='postgres',
    user='postgres',
    password='secret',
    port=7722,
)


# TODO: There must be a better way to deal with local vs CI differences
@pytest.fixture(scope="module")
def container():
    container = None
    if running_locally():
        client = docker.from_env()
        docker_file = _HERE / '../docker/postgres11'
        client.images.build(path=str(docker_file), tag='pg11_test_image')
        container = client.containers.run('pg11_test_image', detach=True,
                                          ports={5432: 7722}, name='pg11_etl1_db')
        while not postgres_is_ready():
            sleep(1)
    yield container if container else None
    if running_locally():
        container.remove(force=True)


def running_locally():
    if 'RUNNING_CI' in os.environ:
        return False
    return True


def postgres_is_ready():
    ready = False
    try:
        conn = psycopg2.connect(f"host=localhost dbname=postgres user=postgres "
                                f"port={str(db_config.port)} password=secret")
        conn.close()
        ready = True
    except psycopg2.OperationalError as e:
        pass
    return ready


@pytest.mark.usefixtures("container")
def test_db_does_not_exist(caplog):
    uri = f'postgresql://postgres:secret@localhost:{db_config.port}/foobar'
    assert not Database.can_connect(uri)
    assert 'Database "foobar" does not exist' in caplog.text

#
# def test_connection_invalid(database, caplog):
#     uri = f'postgresql://postgres:secret@localhost:123456/etl1'
#     assert not Database.can_connect(uri)
#     assert 'Database "etl1" does not exist' not in caplog.text
#
#
# def test_db_exists(database):
#     uri = f'postgresql://postgres:secret@localhost:{db_config.port}/etl1'
#     assert Database.can_connect(uri)
#
#
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
