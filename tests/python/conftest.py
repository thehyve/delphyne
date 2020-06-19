import os
from collections import namedtuple
from pathlib import Path
from time import sleep

import docker
import psycopg2
import pytest

_HERE = Path(__file__).parent


DbConfig = namedtuple('DbConfig', ['host', 'dbname', 'user', 'password', 'port'])

default_db_config = DbConfig(
    host='localhost',
    dbname='postgres',
    user='postgres',
    password='secret',
    port=7722,
)


@pytest.fixture(scope='module')
def db_config() -> DbConfig:
    return default_db_config


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
    yield container  # if container else None
    if running_locally():
        container.remove(force=True)


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
    except psycopg2.OperationalError as e:
        pass
    return ready
