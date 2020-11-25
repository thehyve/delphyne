from typing import Optional, Dict

from pydantic import BaseModel, validator, SecretStr

from ...cdm._schema_placeholders import VOCAB_SCHEMA, CDM_SCHEMA


_REQUIRED_SCHEMAS = [VOCAB_SCHEMA, CDM_SCHEMA]


class _DataBase(BaseModel):
    host: str
    port: int
    database_name: str
    username: str
    password: Optional[SecretStr]

    @validator('password', always=True)
    def missing_password(cls, password):
        if password is None:
            password = SecretStr('')
        return password


class _RunOptions(BaseModel):
    skip_vocabulary_loading: bool
    skip_custom_vocabulary_loading: bool
    skip_source_to_concept_map_loading: bool
    write_reports: bool


class MainConfig(BaseModel):
    database: _DataBase
    schema_translate_map: Dict[str, str]
    run_options: _RunOptions
    sql_parameters: Optional[Dict[str, str]]

    @validator('schema_translate_map')
    def check_required_schemas(cls, schema_map: Dict[str, str]) -> Dict[str, str]:
        for schema in _REQUIRED_SCHEMAS:
            if schema not in schema_map:
                raise ValueError(f'Missing required key in schema_translate_map: {schema}')
        return schema_map

    @validator('schema_translate_map', 'sql_parameters')
    def no_empty_strings(cls, str_dict: Optional[Dict[str, str]]) -> Optional[Dict[str, str]]:
        if str_dict is None:
            return str_dict
        for k, v in str_dict.items():
            if k == '' or v == '':
                raise ValueError(f'Strings cannot be empty: {k}: {v}')
        return str_dict

    @validator('schema_translate_map', 'sql_parameters')
    def values_are_not_also_keys(cls,
                                 str_dict: Optional[Dict[str, str]]
                                 ) -> Optional[Dict[str, str]]:
        # Make sure that a value is not used in another mapping as a
        # key.
        if str_dict is None:
            return str_dict
        for k, v in str_dict.items():
            if v in str_dict and not v == k:
                raise ValueError(f'Mapping value used as key: {v}')
        return str_dict
