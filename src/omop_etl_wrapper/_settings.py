"""Module for internal use of default settings."""

from .cdm._defaults import VOCAB_SCHEMA, CDM_SCHEMA


default_sql_parameters = {
    'vocab_schema': VOCAB_SCHEMA,
    'source_schema': 'source',
    'target_schema': CDM_SCHEMA,
}
