import pytest
from src.delphyne import Wrapper
from src.delphyne.model.mapping import CodeMapping, MappingDict

from tests.python.conftest import docker_not_available
from tests.python.model.mapping.load_concept_relationship import load_concept_relationship


pytestmark = pytest.mark.skipif(condition=docker_not_available(),
                                reason='Docker daemon is not running')


@pytest.mark.usefixtures("test_db")
@pytest.fixture(scope='function')
def cdm600_wrapper_with_loaded_relationships(cdm600_wrapper_with_tables_created: Wrapper) \
        -> Wrapper:
    """cdm600 wrapper with tables created and populated with test
    concept relationships."""
    wrapper = cdm600_wrapper_with_tables_created
    wrapper.db.constraint_manager.drop_all_constraints()
    load_concept_relationship(wrapper=wrapper)
    wrapper.db.constraint_manager.add_all_constraints()
    return wrapper


@pytest.fixture(scope='function')
def mapping_dictionary(cdm600_wrapper_with_loaded_relationships: Wrapper):

    wrapper = cdm600_wrapper_with_loaded_relationships
    map_dict = wrapper.code_mapper.generate_code_mapping_dictionary(
        vocabulary_id='SOURCE')
    return map_dict


@pytest.mark.usefixtures("container", "test_db")
def test_unknown_source_code(mapping_dictionary: MappingDict):

    map_dict = mapping_dictionary

    # source code not in vocabularies
    result_no_code = map_dict.lookup('UNKNOWN CODE')
    result_no_code_concept_only = map_dict.lookup('UNKNOWN CODE', target_concept_id_only=True)

    expected_result_no_code = CodeMapping()
    expected_result_no_code.source_concept_code = 'UNKNOWN CODE'
    expected_result_no_code.source_concept_id = 0
    expected_result_no_code.target_concept_id = 0
    assert len(result_no_code) == 1
    for attr, value in expected_result_no_code.__dict__.items():
        assert getattr(result_no_code[0], attr) == value
    assert result_no_code_concept_only == [0]


@pytest.mark.usefixtures("container", "test_db")
def test_code_with_no_match(mapping_dictionary: MappingDict):

    map_dict = mapping_dictionary

    # non-standard code with no mapping to standard code
    result_no_match = map_dict.lookup('SOURCE_1')
    result_no_match_concept_only = map_dict.lookup('SOURCE_1', target_concept_id_only=True)

    expected_result_no_match = CodeMapping()
    expected_result_no_match.source_concept_code = 'SOURCE_1'
    expected_result_no_match.source_concept_id = 1
    expected_result_no_match.source_concept_name = 'Non-standard concept with no mapping ' \
                                                   'to standard concept'
    expected_result_no_match.source_vocabulary_id = 'SOURCE'
    expected_result_no_match.target_concept_code = None
    expected_result_no_match.target_concept_id = 0
    expected_result_no_match.target_concept_name = None
    expected_result_no_match.target_vocabulary_id = None

    assert len(result_no_match) == 1
    for attr, value in expected_result_no_match.__dict__.items():
        assert getattr(result_no_match[0], attr) == value
    assert result_no_match_concept_only == [0]


@pytest.mark.usefixtures("container", "test_db")
def test_code_with_one_match(mapping_dictionary: MappingDict):

    map_dict = mapping_dictionary

    # non-standard code with one mapping to standard code
    result_1_match = map_dict.lookup('SOURCE_2')
    result_1_match_concept_only = map_dict.lookup('SOURCE_2', target_concept_id_only=True)

    expected_result_1_match = CodeMapping()
    expected_result_1_match.source_concept_code = 'SOURCE_2'
    expected_result_1_match.source_concept_id = 2
    expected_result_1_match.source_concept_name = 'Non-standard concept with 1 mapping ' \
                                                  'to standard concept'
    expected_result_1_match.source_vocabulary_id = 'SOURCE'
    expected_result_1_match.target_concept_code = 'TARGET_1'
    expected_result_1_match.target_concept_id = 4
    expected_result_1_match.target_concept_name = 'Standard concept 1'
    expected_result_1_match.target_vocabulary_id = 'TARGET'

    assert len(result_1_match) == 1
    for attr, value in expected_result_1_match.__dict__.items():
        assert getattr(result_1_match[0], attr) == value
    assert result_1_match_concept_only == [4]


@pytest.mark.usefixtures("container", "test_db")
def test_code_with_multiple_matches(mapping_dictionary: MappingDict):

    map_dict = mapping_dictionary

    # non-standard code with multiple mappings to standard code
    result_multi_match = map_dict.lookup('SOURCE_3')
    result_multi_match_concept_only = map_dict.lookup('SOURCE_3', target_concept_id_only=True)

    expected_result_match_1 = CodeMapping()
    expected_result_match_1.source_concept_code = 'SOURCE_3'
    expected_result_match_1.source_concept_id = 3
    expected_result_match_1.source_concept_name = 'Non-standard concept with multiple mappings ' \
                                                  'to standard concept'
    expected_result_match_1.source_vocabulary_id = 'SOURCE'
    expected_result_match_1.target_concept_code = 'TARGET_1'
    expected_result_match_1.target_concept_id = 4
    expected_result_match_1.target_concept_name = 'Standard concept 1'
    expected_result_match_1.target_vocabulary_id = 'TARGET'

    expected_result_match_2 = CodeMapping()
    expected_result_match_2.source_concept_code = 'SOURCE_3'
    expected_result_match_2.source_concept_id = 3
    expected_result_match_2.source_concept_name = 'Non-standard concept with multiple mappings ' \
                                                  'to standard concept'
    expected_result_match_2.source_vocabulary_id = 'SOURCE'
    expected_result_match_2.target_concept_code = 'TARGET_2'
    expected_result_match_2.target_concept_id = 5
    expected_result_match_2.target_concept_name = 'Standard concept 2'
    expected_result_match_2.target_vocabulary_id = 'TARGET'

    assert len(result_multi_match) == 2
    for attr, value in expected_result_match_1.__dict__.items():
        assert getattr(result_multi_match[0], attr) == value
    for attr, value in expected_result_match_2.__dict__.items():
        assert getattr(result_multi_match[1], attr) == value
    assert result_multi_match_concept_only == [4, 5]


@pytest.mark.usefixtures("container", "test_db")
def test_code_mapping_to_invalid_standard_concepts(mapping_dictionary: MappingDict):

    map_dict = mapping_dictionary

    # non-standard code with no mapping to standard code
    result_no_match = map_dict.lookup('SOURCE_4')

    expected_result_no_match = CodeMapping()
    expected_result_no_match.source_concept_code = 'SOURCE_4'
    expected_result_no_match.source_concept_id = 6
    expected_result_no_match.source_concept_name = 'Non-standard concept with mappings ' \
                                                   'to invalid standard concepts'
    expected_result_no_match.source_vocabulary_id = 'SOURCE'
    expected_result_no_match.target_concept_code = None
    expected_result_no_match.target_concept_id = 0
    expected_result_no_match.target_concept_name = None
    expected_result_no_match.target_vocabulary_id = None

    assert len(result_no_match) == 1
    for attr, value in expected_result_no_match.__dict__.items():
        assert getattr(result_no_match[0], attr) == value


@pytest.mark.usefixtures("container", "test_db")
def test_code_mapping_to_non_standard_concept(mapping_dictionary: MappingDict):

    map_dict = mapping_dictionary

    # non-standard code with no mapping to standard code
    result_no_match = map_dict.lookup('SOURCE_5')

    expected_result_no_match = CodeMapping()
    expected_result_no_match.source_concept_code = 'SOURCE_5'
    expected_result_no_match.source_concept_id = 9
    expected_result_no_match.source_concept_name = 'Non-standard concept with mapping ' \
                                                   'to non-standard concept'
    expected_result_no_match.source_vocabulary_id = 'SOURCE'
    expected_result_no_match.target_concept_code = None
    expected_result_no_match.target_concept_id = 0
    expected_result_no_match.target_concept_name = None
    expected_result_no_match.target_vocabulary_id = None

    assert len(result_no_match) == 1
    for attr, value in expected_result_no_match.__dict__.items():
        assert getattr(result_no_match[0], attr) == value
