import logging
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest
from src.delphyne import Wrapper

from tests.python.conftest import docker_not_available

pytestmark = pytest.mark.skipif(condition=docker_not_available(),
                                reason='Docker daemon is not running')


@pytest.fixture(scope='session')
def base_custom_vocab_dir(test_data_dir: Path) -> Path:
    return test_data_dir / 'custom_vocabs'


@contextmanager
def mock_custom_vocab_path(vocab_dir: Path, custom_vocab_dir_name: str):
    """Mock global variable CUSTOM_VOCAB_DIR."""
    custom_vocab_dir = vocab_dir / custom_vocab_dir_name
    with patch('src.delphyne.model.custom_vocab.custom_vocab_loader.CUSTOM_VOCAB_DIR',
               custom_vocab_dir):
        yield


@pytest.mark.usefixtures("container", "test_db")
def test_custom_vocab_exceptions(cdm600_wrapper_with_tables_created: Wrapper,
                                 base_custom_vocab_dir: Path,
                                 caplog):

    wrapper = cdm600_wrapper_with_tables_created

    with mock_custom_vocab_path(base_custom_vocab_dir, 'dir_not_found'):
        message = 'dir_not_found folder not found'
        with pytest.raises(FileNotFoundError, match=message):
            wrapper.vocab_manager.custom_vocabularies.load()

    with mock_custom_vocab_path(base_custom_vocab_dir, 'no_valid_files'):
        with caplog.at_level(logging.ERROR):
            wrapper.vocab_manager.custom_vocabularies.load()
        assert "No vocabulary.tsv file found" in caplog.text
        assert "No concept.tsv file found" in caplog.text

    with mock_custom_vocab_path(base_custom_vocab_dir, 'no_valid_files'):
        with caplog.at_level(logging.INFO):
            wrapper.vocab_manager.custom_vocabularies.load()
        assert "No concept_class.tsv file found" in caplog.text

    # with mock_custom_vocab_path(base_custom_vocab_dir, 'bad_version_file'):
    #     message = 'stcm_versions.tsv may not contain empty values'
    #     with pytest.raises(ValueError, match=message):
    #         wrapper.vocab_manager.custom_vocabularies.load()
    #
    # with mock_custom_vocab_path(base_custom_vocab_dir, 'stcm1'):
    #     message = 'MY_VOCAB1 is not present in the vocabulary table'
    #     with pytest.raises(ValueError, match=message):
    #         wrapper.vocab_manager.custom_vocabularies.load()
