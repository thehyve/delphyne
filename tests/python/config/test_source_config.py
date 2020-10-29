from pathlib import Path
from typing import Dict

import pytest
from pydantic import ValidationError
from src.omop_etl_wrapper.config.models import SourceConfig


def test_source_folder_must_exist(source_config: Dict):
    source_config['source_data_folder'] = '/a/bad/path/that/should/not/exist'
    with pytest.raises(ValidationError) as excinfo:
        SourceConfig(**source_config)
    assert 'does not exist' in str(excinfo.value)


def test_show_warning_when_config_file_not_found(source_config: Dict, caplog):
    source_config['source_files']['not_there.csv'] = {}
    SourceConfig(**source_config)
    assert 'Source file "not_there.csv" not found' in caplog.text


def test_show_warning_when_source_dir_is_empty(source_config: Dict,
                                               source_data_test_dir: Path,
                                               caplog
                                               ):
    source_data_dir = Path(source_data_test_dir / 'empty_dir').resolve()
    source_config['source_data_folder'] = source_data_dir
    source_config['source_files'] = {}
    SourceConfig(**source_config)
    assert 'No source files were found at' in caplog.text