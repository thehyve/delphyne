"""
Container for all relative path constants, as they are expected to be
present in the template repository.
"""

# TODO: Add expected paths for:
#  omop vocabularies dir

from pathlib import Path

# Config file used by setup_logging
LOG_CONFIG_PATH = Path('./config/logging.yml')
SOURCE_DATA_CONFIG_PATH = Path('./config/source_config.yml')

# All log files will be written here. Will be created if not present.
LOG_OUTPUT_DIR = Path('./logs')

RESOURCES_DIR = Path('./resources')
STCM_DIR = RESOURCES_DIR / 'source_to_concept'
CUSTOM_VOCAB_DIR = RESOURCES_DIR / 'custom_vocabularies'
