import logging
from contextlib import contextmanager
from pathlib import Path
from typing import List, Tuple
from unittest.mock import patch

import pytest
from sqlalchemy.exc import InvalidRequestError
from src.delphyne import Wrapper

from tests.python.cdm.cdm600 import SourceToConceptMapVersion, SourceToConceptMap
from tests.python.conftest import docker_not_available
from tests.python.model.stcm.load_vocab_data import (load_minimal_vocabulary,
                                                     load_custom_vocab_records)

pytestmark = pytest.mark.skipif(condition=docker_not_available(),
                                reason='Docker daemon is not running')


def get_all_stcm_records(wrapper: Wrapper) -> List[Tuple]:
    """
    Return list of source_to_concept_map rows as tuples, containing only
    source_code and source_vocabulary_id.
    """
    with wrapper.db.session_scope() as session:
        records = session.query(SourceToConceptMap).all()
        return [(r.source_code, r.source_vocabulary_id) for r in records]


@pytest.fixture(scope='session')
def base_stcm_dir(test_data_dir: Path) -> Path:
    return test_data_dir / 'stcm'


@pytest.mark.usefixtures("test_db")
@pytest.fixture(scope='function')
def cdm600_wrapper_no_constraints(cdm600_wrapper_with_tables_created: Wrapper) -> Wrapper:
    """cdm600 wrapper with tables created, but without constraints."""
    wrapper = cdm600_wrapper_with_tables_created
    wrapper.db.constraint_manager.drop_all_constraints()
    return wrapper


@contextmanager
def mock_stcm_paths(base_dir: Path, stcm_dir_name: str):
    """Mock global variables STCM_DIR and STCM_VERSION_FILE."""
    stcm_dir = base_dir / stcm_dir_name
    version_file = stcm_dir / 'stcm_versions.tsv'
    with patch('src.delphyne.model.stcm.stcm_loader.STCM_DIR', stcm_dir), \
         patch('src.delphyne.model.stcm.stcm_loader.STCM_VERSION_FILE', version_file):
        yield


@pytest.mark.usefixtures("container", "test_db")
def test_stcm_exceptions(cdm600_wrapper_no_constraints: Wrapper, base_stcm_dir: Path, caplog):
    wrapper = cdm600_wrapper_no_constraints

    with mock_stcm_paths(base_stcm_dir, 'dir_not_found'):
        message = 'dir_not_found folder not found'
        with pytest.raises(FileNotFoundError, match=message):
            wrapper.vocab_manager.stcm.load()

    with mock_stcm_paths(base_stcm_dir, 'no_version_file'):
        message = 'source to concept map version file not found'
        with pytest.raises(FileNotFoundError, match=message):
            wrapper.vocab_manager.stcm.load()

    with mock_stcm_paths(base_stcm_dir, 'bad_version_file'):
        message = 'stcm_versions.tsv may not contain empty values'
        with pytest.raises(ValueError, match=message):
            wrapper.vocab_manager.stcm.load()

    with mock_stcm_paths(base_stcm_dir, 'stcm1'):
        message = 'MY_VOCAB1 is not present in the vocabulary table'
        with pytest.raises(ValueError, match=message):
            wrapper.vocab_manager.stcm.load()

    with mock_stcm_paths(base_stcm_dir, 'stcm1'):
        wrapper.drop_cdm(tables_to_drop=[SourceToConceptMapVersion.__table__])
        with pytest.raises(InvalidRequestError):
            wrapper.vocab_manager.stcm.load()
        assert 'Table vocab.source_to_concept_map_version does not exist' in caplog.text


@pytest.mark.usefixtures("container", "test_db")
def test_load_stcm(cdm600_wrapper_no_constraints: Wrapper, base_stcm_dir: Path, caplog):
    wrapper = cdm600_wrapper_no_constraints
    load_minimal_vocabulary(wrapper=wrapper)
    load_custom_vocab_records(wrapper=wrapper, vocab_ids=['MY_VOCAB1', 'MY_VOCAB2'])
    wrapper.db.constraint_manager.add_all_constraints()

    with mock_stcm_paths(base_stcm_dir, 'stcm1'):
        wrapper.vocab_manager.stcm.load()
        records = get_all_stcm_records(wrapper)
        assert records == [('code1', 'MY_VOCAB1')]

    # New MY_VOCAB2 vocabulary, MY_VOCAB1 unchanged
    with mock_stcm_paths(base_stcm_dir, 'stcm2'), caplog.at_level(logging.INFO):
        wrapper.vocab_manager.stcm.load()
        records = get_all_stcm_records(wrapper)
        assert records == [('code1', 'MY_VOCAB1'), ('code2', 'MY_VOCAB2')]
    assert "Skipping file MY_VOCAB1_stcm.csv" in caplog.text
    assert "already loaded under the current version: [('MY_VOCAB1', 1)]" in caplog.text
