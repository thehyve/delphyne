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

# Directory containing SQL scripts
SQL_TRANSFORMATIONS_DIR = Path('./src/main/sql')

RESOURCES_DIR = Path('./resources')
# Directory containing all source_to_concept_map files
STCM_DIR = RESOURCES_DIR / 'source_to_concept'
# File containing the versioning info of the STCM mappings
STCM_VERSION_FILE = STCM_DIR / 'stcm_versions.tsv'
# Directory containing all custom vocabulary tables
CUSTOM_VOCAB_DIR = RESOURCES_DIR / 'custom_vocabularies'
# Directory containing all standard vocabulary tables
STANDARD_VOCAB_DIR = RESOURCES_DIR / 'standard_vocabularies'
