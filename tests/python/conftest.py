import os
from collections import namedtuple
from contextlib import contextmanager
from pathlib import Path
from typing import Dict

import docker
import psycopg2
import pytest
from docker.errors import DockerException
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database, drop_database
from src.delphyne.config.models import MainConfig
from src.delphyne.util.io import read_yaml_file
from src.delphyne.wrapper import Wrapper
from time import sleep

from .cdm import cdm531, cdm600

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


@pytest.fixture(scope='session')
def test_data_dir() -> Path:
    """Return Path of the folder containing the test data files."""
    return _HERE.parent / 'test_data'


def running_locally() -> bool:
    if 'RUNNING_CI' in os.environ:
        return False
    return True


def docker_daemon_is_running() -> bool:
    try:
        client = docker.from_env()
        client.info()
    except (ConnectionRefusedError, DockerException):
        return False
    return True


def docker_not_available() -> bool:
    return running_locally() and not docker_daemon_is_running()


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
def module_scope_db_config() -> Dict:
    config_file = _CONFIG_DIR / 'module_scope_db_config.yml'
    return read_yaml_file(config_file)


@pytest.fixture(scope='session')
def default_main_config(default_run_config) -> MainConfig:
    return MainConfig(**default_run_config)


@pytest.fixture(scope='session')
def module_scope_db_main_config(module_scope_db_config) -> MainConfig:
    return MainConfig(**module_scope_db_config)


def _get_db_string_from_main_config(config: MainConfig) -> str:
    db_config = config.database
    hostname = db_config.host
    port = db_config.port
    database = db_config.database_name
    username = db_config.username
    password = db_config.password.get_secret_value()
    return f'postgresql://{username}:{password}@{hostname}:{port}/{database}'


@pytest.fixture(scope='session')
def test_db_uri(default_main_config: MainConfig) -> str:
    """Return DB uri for the function-scope DB."""
    return _get_db_string_from_main_config(default_main_config)


@pytest.fixture(scope='session')
def test_db_module_scope_uri(module_scope_db_main_config: MainConfig) -> str:
    """Return DB uri for the module-scope DB."""
    return _get_db_string_from_main_config(module_scope_db_main_config)


@pytest.fixture(scope='function')
def test_db(container, test_db_uri: str) -> None:
    """Function-scope test DB"""
    with _test_db_fixture_implementation(test_db_uri) as result:
        yield result


@pytest.fixture(scope='module')
def test_db_module(container, test_db_module_scope_uri: str) -> None:
    """Module-scope test DB"""
    with _test_db_fixture_implementation(test_db_module_scope_uri) as result:
        yield result


@contextmanager
def _test_db_fixture_implementation(test_db_uri: str):
    engine = create_engine(test_db_uri)
    create_database(engine.url)
    yield
    drop_database(engine.url)


@pytest.fixture(scope='function')
def wrapper_cdm531(test_db, default_main_config: MainConfig) -> Wrapper:
    wrapper = Wrapper(default_main_config, cdm531)
    return wrapper


@pytest.fixture(scope='function')
def wrapper_cdm600(test_db, default_main_config: MainConfig) -> Wrapper:
    wrapper = Wrapper(default_main_config, cdm600)
    return wrapper


@pytest.fixture(scope='function')
def cdm531_wrapper_with_tables_created(test_db, wrapper_cdm531: Wrapper) -> Wrapper:
    wrapper_cdm531.create_schemas()
    wrapper_cdm531.create_cdm()
    return wrapper_cdm531


@pytest.fixture(scope='function')
def cdm600_wrapper_with_tables_created(test_db, wrapper_cdm600: Wrapper) -> Wrapper:
    wrapper_cdm600.create_schemas()
    wrapper_cdm600.create_cdm()
    return wrapper_cdm600


@pytest.fixture(scope="session")
def source_config_dir(test_data_dir: Path) -> Path:
    return test_data_dir / 'source_data_configs'


@pytest.fixture(scope="session")
def source_data_test_dir(test_data_dir: Path) -> Path:
    """Directory holding subdirectories with source data test files."""
    return test_data_dir / 'source_data_files'


@pytest.fixture
def source_config(source_config_dir: Path, source_data_test_dir: Path) -> Dict:
    source_config_path = source_config_dir / 'source_config.yml'
    source_config = read_yaml_file(source_config_path)
    # Insert the resolved path of the source_data_dir in the config
    source_data_dir = Path(source_data_test_dir / 'test_dir1').resolve()
    source_config['source_data_folder'] = source_data_dir
    return source_config
