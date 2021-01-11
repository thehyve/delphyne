from contextlib import contextmanager
from copy import copy
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import MetaData
from src.delphyne import Wrapper

import tests.python.cdm.cdm600 as cdm
from tests.python.conftest import docker_not_available

pytestmark = pytest.mark.skipif(condition=docker_not_available(),
                                reason='Docker daemon is not running')


@pytest.fixture(scope='session')
def base_standard_vocab_dir(test_data_dir: Path) -> Path:
    return test_data_dir / 'standard_vocabs'


@contextmanager
def mock_standard_vocab_paths(base_dir: Path, standard_vocab_dir_name: str):
    """Mock global variable STANDARD_VOCAB_DIR."""
    vocab_dir = base_dir / standard_vocab_dir_name
    import_path = 'src.delphyne.model.standard_vocab.standard_vocab_loader.STANDARD_VOCAB_DIR'
    with patch(import_path, vocab_dir):
        yield


def insert_dummy_domain_record(wrapper: Wrapper):
    with wrapper.db.session_scope() as session:
        domain_record = cdm.Domain()
        domain_record.domain_id = 1
        domain_record.domain_concept_id = 0
        domain_record.domain_name = 'dkOGLGRWxqU'
        session.add(domain_record)


@pytest.mark.usefixtures("container", "test_db")
def test_standard_vocab_exceptions(cdm600_wrapper_with_tables_created: Wrapper,
                                   base_standard_vocab_dir: Path,
                                   ):
    wrapper = cdm600_wrapper_with_tables_created

    with mock_standard_vocab_paths(base_standard_vocab_dir, 'dir_not_found'):
        message = 'standard vocabulary folder not found'
        with pytest.raises(FileNotFoundError, match=message):
            wrapper.vocab_manager.load_standard_vocabularies()

    with mock_standard_vocab_paths(base_standard_vocab_dir, 'vocab_incomplete'):
        message = 'No corresponding file was found for table "concept"'
        with pytest.raises(FileNotFoundError, match=message):
            wrapper.vocab_manager.load_standard_vocabularies()

    # Temporarily remove the Base metadata, to check whether an
    # exception is raised about vocabulary tables not being part of
    # Base. Restore the original metadata afterwards.
    original_metadata = copy(wrapper.db.base.metadata)
    wrapper.db.base.metadata = MetaData()
    with mock_standard_vocab_paths(base_standard_vocab_dir, 'vocab1'):
        with pytest.raises(ValueError, match='Missing table "concept"'):
            wrapper.vocab_manager.load_standard_vocabularies()
    wrapper.db.base.metadata = original_metadata

    # pre-existing records in a vocabulary table will raise an exception
    wrapper.db.constraint_manager.drop_all_constraints()
    insert_dummy_domain_record(wrapper)
    with mock_standard_vocab_paths(base_standard_vocab_dir, 'vocab1'):
        with pytest.raises(ValueError, match='Table "domain" is not empty'):
            wrapper.vocab_manager.load_standard_vocabularies()


@pytest.mark.usefixtures("container", "test_db")
def test_standard_vocab_loading(cdm600_wrapper_with_tables_created: Wrapper,
                                base_standard_vocab_dir: Path,
                                ):
    wrapper = cdm600_wrapper_with_tables_created

    with mock_standard_vocab_paths(base_standard_vocab_dir, 'vocab1'):
        wrapper.vocab_manager.load_standard_vocabularies()

    with wrapper.db.session_scope() as session:
        # All vocabulary files contain a header and one data row
        assert session.query(cdm.Concept).count() == 1
        assert session.query(cdm.ConceptAncestor).count() == 1
        assert session.query(cdm.ConceptClass).count() == 1
        assert session.query(cdm.ConceptRelationship).count() == 1
        assert session.query(cdm.ConceptSynonym).count() == 1
        assert session.query(cdm.Domain).count() == 1
        assert session.query(cdm.Relationship).count() == 1
        assert session.query(cdm.Vocabulary).count() == 1
        # DRUG_STRENGTH.csv contains only a header, so the database
        # table should remain empty
        assert session.query(cdm.DrugStrength).count() == 0
