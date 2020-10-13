"""
Container for all relative path constants, as they are expected to be
present in the template repository.
"""

from pathlib import Path

# Config file used by setup_logging
LOG_CONFIG_PATH = Path('./config/logging.yml')
SOURCE_DATA_CONFIG_PATH = Path('./config/source_config.yml')

# Directory containing all source_to_concept_map files
STCM_DIR = Path('./resources/source_to_concept/')

# All log files will be written here. Will be created if not present.
LOG_OUTPUT_DIR = Path('./logs')

# TODO: Add expected paths for:
#  custom vocabularies dir
#  omop vocabularies dir

