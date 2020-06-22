import os
from collections import namedtuple
from pathlib import Path
from time import sleep
from typing import Dict

import docker
import psycopg2
import pytest
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, drop_database

from src.omop_etl_wrapper.util.io import read_yaml_file
from src.omop_etl_wrapper.wrapper import Wrapper

_HERE = Path(__file__).parent
_PG11_DOCKER_FILE = _HERE / '../docker/postgres11'
_CONFIG_DIR = _HERE / '../test_data/run_configs/'


DbConfig = namedtuple('DbConfig', ['host', 'dbname', 'user', 'password', 'port'])

default_db_config = DbConfig(
    host='localhost',
    dbname='postgres',
    user='postgres',
    password='secret',
    port=7722,
)


def running_locally() -> bool:
    if 'RUNNING_CI' in os.environ:
        return False
    return True


def postgres_is_ready() -> bool:
    ready = False
    try:
        conn = psycopg2.connect(f"host=localhost dbname=postgres user=postgres "
                                f"port={str(default_db_config.port)} password=secret")
        conn.close()
        ready = True
    except psycopg2.OperationalError:
        pass
    return ready


@pytest.fixture(scope='session')
def db_config() -> DbConfig:
    return default_db_config


@pytest.fixture(scope="session")
def container():
    container = None
    try:
        if running_locally():
            client = docker.from_env()
            client.images.build(path=str(_PG11_DOCKER_FILE), tag='pg11_test_image')
            container = client.containers.run('pg11_test_image', detach=True,
                                              ports={5432: 7722}, name='pg11_etl1_db')
            while not postgres_is_ready():
                sleep(1)
        yield container
    finally:
        if running_locally():
            container.remove(force=True)


@pytest.fixture(scope='session')
def default_run_config() -> Dict:
    config_file = _CONFIG_DIR / 'default.yml'
    return read_yaml_file(config_file)


@pytest.fixture(scope='session')
def test_db_uri(default_run_config) -> str:
    db_config = default_run_config['database']
    hostname = db_config['host']
    port = db_config['port']
    database = db_config['database_name']
    username = db_config['username']
    password = db_config['password']
    return f'postgresql://{username}:{password}@{hostname}:{port}/{database}'


@pytest.mark.usefixtures("container")
@pytest.fixture(scope='function')
def test_db(test_db_uri) -> None:
    engine = create_engine(test_db_uri)
    create_database(engine.url)
    yield
    drop_database(engine.url)


@pytest.mark.usefixtures("test_db")
@pytest.fixture(scope='function')
def wrapper_cdm531(default_run_config):
    wrapper = Wrapper(default_run_config)
    yield wrapper


@pytest.mark.usefixtures("test_db")
@pytest.fixture(scope='function')
def wrapper_cdm600(default_run_config):
    default_run_config['run_options']['cdm'] = 'cdm600'
    wrapper = Wrapper(default_run_config)
    yield wrapper
