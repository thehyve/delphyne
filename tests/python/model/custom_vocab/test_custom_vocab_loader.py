import logging
import pytest
import re
from contextlib import contextmanager
from pathlib import Path
from sqlalchemy.exc import IntegrityError
from typing import List, Tuple, Union
from unittest.mock import patch

from src.delphyne import Wrapper
from src.delphyne.config.models import MainConfig

from tests.python.cdm import cdm600
from tests.python.conftest import docker_not_available
from tests.python.model.custom_vocab.load_vocab_data import load_minimal_vocabulary, \
    load_custom_vocab_records, load_custom_class_records, load_custom_concept_records

pytestmark = pytest.mark.skipif(condition=docker_not_available(),
                                reason='Docker daemon is not running')


def get_custom_vocab_records(wrapper: Wrapper) -> List[str]:
    """
    Return list of vocabulary_versions for custom vocabularies only.
    """
    with wrapper.db.session_scope() as session:
        records = session.query(cdm600.Vocabulary) \
            .filter(cdm600.Vocabulary.vocabulary_concept_id == 0) \
            .all()
        records = [r.vocabulary_version for r in records]
        records.sort()
        return records


def get_custom_class_records(wrapper: Wrapper) -> List[str]:
    """
    Return list of concept_class_names for custom classes only.
    """
    with wrapper.db.session_scope() as session:
        records = session.query(cdm600.ConceptClass) \
            .filter(cdm600.ConceptClass.concept_class_concept_id == 0) \
            .all()
        records = [r.concept_class_name for r in records]
        records.sort()
        return records


def get_custom_concept_records(wrapper: Wrapper, concept_id_only: bool = True
                               ) -> Union[List[int], List[Tuple[int, str]]]:
    """
    Return list of concept_class_names for custom classes only.
    """
    with wrapper.db.session_scope() as session:
        records = session.query(cdm600.Concept) \
            .filter(cdm600.Concept.concept_id > 2000000000) \
            .all()
        if concept_id_only:
            records = [r.concept_id for r in records]
        else:
            records = [(r.concept_id, r.concept_class_id) for r in records]
        records.sort()
        return records


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


@pytest.fixture(scope='function')
def cdm600_wrapper_with_empty_tables(test_db_module, module_scope_db_main_config: MainConfig
                                     ) -> Wrapper:
    """cdm600 wrapper with all tables created."""
    wrapper = Wrapper(module_scope_db_main_config, cdm600)
    wrapper.create_schemas()
    wrapper.create_cdm()
    return wrapper


@pytest.fixture(scope='function')
def cdm600_with_minimal_vocabulary_tables(cdm600_wrapper_with_empty_tables) -> Wrapper:
    """cdm600 wrapper with minimal set of data in vocabulary tables."""
    wrapper = cdm600_wrapper_with_empty_tables
    wrapper.db.constraint_manager.drop_all_constraints()
    with wrapper.db.session_scope() as session:
        session.query(cdm600.Concept).delete()
        session.query(cdm600.ConceptClass).delete()
        session.query(cdm600.Vocabulary).delete()
        session.query(cdm600.Domain).delete()
    load_minimal_vocabulary(wrapper=wrapper)
    wrapper.db.constraint_manager.add_all_constraints()
    return wrapper


def test_custom_vocab_files_availability(cdm600_wrapper_with_empty_tables,
                                         base_custom_vocab_dir: Path, caplog):

    wrapper = cdm600_wrapper_with_empty_tables

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


def test_custom_vocabulary_quality(cdm600_wrapper_with_empty_tables,
                                   base_custom_vocab_dir: Path, caplog):

    wrapper = cdm600_wrapper_with_empty_tables

    with mock_custom_vocab_path(base_custom_vocab_dir, 'bad_vocab_content'):
        message = re.escape("Vocabulary files"
                            " ['bad_vocabulary.tsv', 'duplicate_vocabulary.tsv']"
                            " contain invalid values")
        with pytest.raises(ValueError, match=message):
            wrapper.vocab_manager.custom_vocabularies.load()

        assert "bad_vocabulary.tsv may not contain an empty vocabulary_id" \
               in caplog.text
        assert "bad_vocabulary.tsv may not contain an empty vocabulary_version" \
               in caplog.text
        assert "bad_vocabulary.tsv may not contain an empty vocabulary_reference" \
               in caplog.text
        assert "bad_vocabulary.tsv may not contain vocabulary_concept_id other than 0" \
               in caplog.text
        # vocabulary duplicated within file
        assert "vocabulary VOCAB1 is duplicated across one or multiple files" in caplog.text
        # vocabulary duplicated between files
        assert "vocabulary VOCAB2 is duplicated across one or multiple files" in caplog.text


def test_custom_concept_class_quality(cdm600_wrapper_with_empty_tables,
                                      base_custom_vocab_dir: Path, caplog):

    wrapper = cdm600_wrapper_with_empty_tables

    with mock_custom_vocab_path(base_custom_vocab_dir, 'bad_class_content'):
        message = re.escape("Concept class files ['bad_concept_class.tsv', "
                            "'duplicate_concept_class.tsv'] contain invalid values")
        with pytest.raises(ValueError, match=message):
            wrapper.vocab_manager.custom_vocabularies.load()

        assert "bad_concept_class.tsv may not contain an empty concept_class_id" \
               in caplog.text
        assert "bad_concept_class.tsv may not contain an empty concept_class_name" \
               in caplog.text
        assert "bad_concept_class.tsv may not containt concept_class_concept_id other than " \
               "0" \
               in caplog.text
        # class duplicated within file
        assert "concept class CLASS1 is duplicated across one or multiple files" \
               in caplog.text
        # class duplicated between files
        assert "concept class CLASS2 is duplicated across one or multiple files" \
               in caplog.text


def test_custom_concept_quality(cdm600_with_minimal_vocabulary_tables,
                                base_custom_vocab_dir: Path, caplog):

    wrapper = cdm600_with_minimal_vocabulary_tables

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


def test_custom_vocab_and_concept_update(cdm600_with_minimal_vocabulary_tables,
                                         base_custom_vocab_dir: Path, caplog):

    wrapper = cdm600_with_minimal_vocabulary_tables

    load_custom_vocab_records(wrapper, ['VOCAB1', 'VOCAB2', 'VOCAB3'])
    load_custom_concept_records(wrapper, {2000000001: ('VOCAB1', ''),
                                          2000000002: ('VOCAB2', ''),
                                          2000000003: ('VOCAB3', '')
                                          })

    with mock_custom_vocab_path(base_custom_vocab_dir, 'custom_vocab_test1'), \
            caplog.at_level(logging.INFO):
        wrapper.vocab_manager.custom_vocabularies.load()

    # VOCAB1 removed, VOCAB2 unchanged, VOCAB3 updated, VOCAB4 new
    assert 'Found obsolete vocabulary version: VOCAB1' in caplog.text
    assert "Skipped records with vocabulary_id values that were already loaded under the" \
           " current version: {'VOCAB2'}"
    assert 'Found new vocabulary version: VOCAB3 : VOCAB3_v1 -> VOCAB3_v2' in caplog.text
    assert 'Found new vocabulary version: VOCAB4 : None -> VOCAB4_v1' in caplog.text

    loaded_vocabs = get_custom_vocab_records(wrapper)
    assert loaded_vocabs == ['VOCAB2_v1', 'VOCAB3_v2', 'VOCAB4_v1']
    # only old concepts associated with unchanged vocabs (VOCAB2)
    # and new concepts associated with new vocabs (VOCAB3, VOCAB4)
    # should be present
    loaded_concepts = get_custom_concept_records(wrapper)
    assert loaded_concepts == [2000000002, 2000000006, 2000000007]


def test_custom_class_and_concept_update(cdm600_with_minimal_vocabulary_tables,
                                         base_custom_vocab_dir: Path, caplog):

    wrapper = cdm600_with_minimal_vocabulary_tables

    load_custom_class_records(wrapper, ['CLASS1', 'CLASS2', 'CLASS3'])
    load_custom_vocab_records(wrapper, ['VOCAB1', 'VOCAB2'])
    load_custom_concept_records(wrapper, {2000000001: ('VOCAB1', 'CLASS1'),
                                          2000000002: ('VOCAB1', 'CLASS2'),
                                          2000000003: ('VOCAB2', 'CLASS3')
                                          })

    with mock_custom_vocab_path(base_custom_vocab_dir, 'custom_vocab_test2'), \
            caplog.at_level(logging.INFO):
        wrapper.vocab_manager.custom_vocabularies.load()

    # CLASS1 removed, CLASS2 unchanged, CLASS3 updated, CLASS4 new
    assert 'Found obsolete concept_class version: CLASS1' in caplog.text
    assert "Skipped records with concept_class_id values that were already loaded under the" \
           " current name: {'CLASS2'}"
    assert 'Found new concept_class version: CLASS3 : CLASS3_v1 -> CLASS3_v2' in caplog.text
    assert 'Found new concept_class version: CLASS4 : None -> CLASS4_v1' in caplog.text

    loaded_classes = get_custom_class_records(wrapper)
    assert loaded_classes == ['CLASS2_v1', 'CLASS3_v2', 'CLASS4_v1']
    # concept1&2 have updated vocabulary (VOCAB1) and have been mapped
    # to new class (CLASS4) (concept1 original CLASS1 has been deleted,
    # concept2 CLASS2 still exists in the database);
    # concept3 has same vocab version (VOCAB2),
    # but class has been updated to v2 (CLASS3)
    loaded_concepts = get_custom_concept_records(wrapper, concept_id_only=False)
    assert loaded_concepts == [(2000000001, 'CLASS4'),
                               (2000000002, 'CLASS4'),
                               (2000000003, 'CLASS3')]


def test_file_prefix_recognition_vocabs_and_classes(cdm600_with_minimal_vocabulary_tables,
                                                    base_custom_vocab_dir: Path, caplog):

    wrapper = cdm600_with_minimal_vocabulary_tables

    load_custom_vocab_records(wrapper, ['VOCAB1', 'VOCAB2', 'VOCAB3', 'VOCAB4'])
    load_custom_class_records(wrapper, ['CLASS1', 'CLASS2'])

    with mock_custom_vocab_path(base_custom_vocab_dir, 'custom_vocab_test3'):
        wrapper.vocab_manager.custom_vocabularies.load()

        loaded_vocabs = get_custom_vocab_records(wrapper)
        loaded_classes = get_custom_class_records(wrapper)

        # vocabularies are updated even if not matching file prefix,
        # also when file prefix corresponds to an unchanged vocabulary
        # (e.g. updated VOCAB4 in file with prefix of unchanged VOCAB3)

        assert "VOCAB1_vocabulary.tsv contains vocabulary_ids that do not match file prefix:" \
               " {'VOCAB2'}" in caplog.text
        assert "VOCAB3_vocabulary.tsv contains vocabulary_ids that do not match file prefix:" \
               " {'VOCAB4'}" in caplog.text
        assert loaded_vocabs == ['VOCAB1_v2', 'VOCAB2_v2', 'VOCAB3_v1', 'VOCAB4_v2']

        # classes are updated irrespective of the file prefix
        assert loaded_classes == ['CLASS1_v2', 'CLASS2_v2']
