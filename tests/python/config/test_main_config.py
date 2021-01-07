from copy import deepcopy
from typing import Dict

import pytest
from pydantic import ValidationError
from src.delphyne.config.models import MainConfig


@pytest.fixture(scope='function')
def default_main_config(default_run_config: Dict) -> Dict:
    return deepcopy(default_run_config)


def test_default_config_is_valid(default_main_config: Dict):
    MainConfig(**default_main_config)


def test_schema_translate_map_can_hold_additional_schemas(default_main_config: Dict):
    schema_map = default_main_config['schema_translate_map']
    schema_map['custom_schema'] = 'my_schema'
    c = MainConfig(**default_main_config)
    expected_schema_keys = {'cdm_schema', 'vocabulary_schema', 'custom_schema'}
    assert c.schema_translate_map.keys() == expected_schema_keys


def test_missing_required_schema_raises_validation_error(default_main_config: Dict):
    schema_map = default_main_config['schema_translate_map']
    del schema_map['cdm_schema']
    with pytest.raises(ValidationError) as error:
        MainConfig(**default_main_config)
    assert 'cdm_schema' in str(error.value)


def test_missing_database_raises_validation_error(default_main_config: Dict):
    del default_main_config['database']
    with pytest.raises(ValidationError) as error:
        MainConfig(**default_main_config)
    assert 'database' in str(error.value)


def test_missing_database_password_set_to_empty_secret_string(default_main_config: Dict):
    del default_main_config['database']['password']
    c = MainConfig(**default_main_config)
    assert c.database.password.get_secret_value() == ''


def test_sql_parameter_value_empty_string(default_main_config: Dict):
    sql_parameters = {'var1': ''}
    default_main_config['sql_parameters'] = sql_parameters
    with pytest.raises(ValidationError) as error:
        MainConfig(**default_main_config)
    assert "Strings cannot be empty: var1:" in str(error.value)


def test_sql_parameter_key_empty_string(default_main_config: Dict):
    sql_parameters = {'var1': 'val1', '': 'val2'}
    default_main_config['sql_parameters'] = sql_parameters
    with pytest.raises(ValidationError) as error:
        MainConfig(**default_main_config)
    assert "Strings cannot be empty: : val2" in str(error.value)


def test_mapping_value_cannot_also_be_key_in_other_pair(default_main_config: Dict):
    sql_parameters = {'key1': 'val1', 'val1': 'val2'}
    default_main_config['sql_parameters'] = sql_parameters
    with pytest.raises(ValidationError) as error:
        MainConfig(**default_main_config)
    assert "Mapping value used as key: val1" in str(error.value)


def test_mapping_value_can_be_same_as_key_within_same_pair(default_main_config: Dict):
    sql_parameters = {'key1': 'key1'}
    default_main_config['sql_parameters'] = sql_parameters
    MainConfig(**default_main_config)
