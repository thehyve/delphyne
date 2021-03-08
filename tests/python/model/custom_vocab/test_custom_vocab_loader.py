import logging
import pytest
import re
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from src.delphyne import Wrapper
from src.delphyne.config.models import MainConfig

from tests.python.cdm import cdm600
from tests.python.conftest import docker_not_available
from tests.python.model.custom_vocab.load_vocab_data import load_minimal_vocabulary

pytestmark = pytest.mark.skipif(condition=docker_not_available(),
                                reason='Docker daemon is not running')


@pytest.fixture(scope='module')
def base_custom_vocab_dir(test_data_dir: Path) -> Path:
    return test_data_dir / 'custom_vocabs'


@contextmanager
def mock_custom_vocab_path(vocab_dir: Path, custom_vocab_dir_name: str):
    """Mock global variable CUSTOM_VOCAB_DIR."""
    custom_vocab_dir = vocab_dir / custom_vocab_dir_name
    with patch('src.delphyne.model.custom_vocab.custom_vocab_loader.CUSTOM_VOCAB_DIR',
               custom_vocab_dir):
        yield


@pytest.fixture(scope='module')
def cdm600_wrapper_with_minimal_contents(test_db_module,
                                         module_scope_db_main_config: MainConfig
                                         ) -> Wrapper:
    """cdm600 wrapper with tables created and populated with minimal
    contents."""
    wrapper = Wrapper(module_scope_db_main_config, cdm600)
    wrapper.create_schemas()
    wrapper.create_cdm()
    wrapper.db.constraint_manager.drop_all_constraints()
    load_minimal_vocabulary(wrapper=wrapper)
    wrapper.db.constraint_manager.add_all_constraints()
    return wrapper


def test_custom_vocab_files_availability(cdm600_wrapper_with_tables_created: Wrapper,
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


def test_custom_vocabulary_quality(cdm600_wrapper_with_tables_created: Wrapper,
                                   base_custom_vocab_dir: Path,
                                   caplog):

    wrapper = cdm600_wrapper_with_tables_created

    with mock_custom_vocab_path(base_custom_vocab_dir, 'bad_vocab_content'):
        message = re.escape("Vocabulary files ['bad_vocabulary.tsv', 'duplicate_vocabulary.tsv']"
                            " contain invalid values")
        with pytest.raises(ValueError, match=message):
            wrapper.vocab_manager.custom_vocabularies.load()

        assert "bad_vocabulary.tsv may not contain an empty vocabulary_id" in caplog.text
        assert "bad_vocabulary.tsv may not contain an empty vocabulary_version" in caplog.text
        assert "bad_vocabulary.tsv may not contain an empty vocabulary_reference" in caplog.text
        assert "bad_vocabulary.tsv may not contain vocabulary_concept_id other than 0" \
               in caplog.text
        # vocabulary duplicated within file
        assert "vocabulary VOCAB1 is duplicated across one or multiple files" in caplog.text
        # vocabulary duplicated between files
        assert "vocabulary VOCAB2 is duplicated across one or multiple files" in caplog.text


def test_custom_concept_class_quality(cdm600_wrapper_with_tables_created: Wrapper,
                                      base_custom_vocab_dir: Path,
                                      caplog):

    wrapper = cdm600_wrapper_with_tables_created

    with mock_custom_vocab_path(base_custom_vocab_dir, 'bad_class_content'):
        message = re.escape("Concept class files ['bad_concept_class.tsv', "
                            "'duplicate_concept_class.tsv'] contain invalid values")
        with pytest.raises(ValueError, match=message):
            wrapper.vocab_manager.custom_vocabularies.load()

        assert "bad_concept_class.tsv may not contain an empty concept_class_id" in caplog.text
        assert "bad_concept_class.tsv may not contain an empty concept_class_name" in caplog.text
        assert "bad_concept_class.tsv may not containt concept_class_concept_id other than 0" \
               in caplog.text
        # class duplicated within file
        assert "concept class CLASS1 is duplicated across one or multiple files" in caplog.text
        # class duplicated between files
        assert "concept class CLASS2 is duplicated across one or multiple files" in caplog.text


def test_custom_concept_quality(cdm600_wrapper_with_minimal_contents: Wrapper,
                                base_custom_vocab_dir: Path,
                                caplog):

    wrapper = cdm600_wrapper_with_minimal_contents

    with mock_custom_vocab_path(base_custom_vocab_dir, 'bad_concept_content'):
        message = re.escape("Concept files ['bad_concept.tsv', 'duplicate_concept.tsv']"
                            " contain invalid values")
        with pytest.raises(ValueError, match=message):
            wrapper.vocab_manager.custom_vocabularies.load()

        assert "bad_concept.tsv must contain concept_ids starting at " \
               "2\'000\'000\'000 (2B+ convention)" in caplog.text
        # concept duplicated within file
        assert "concept 2000000001 is duplicated across one or multiple files" in caplog.text
        # concept duplicated between files
        assert "concept 2000000002 is duplicated across one or multiple files" in caplog.text
