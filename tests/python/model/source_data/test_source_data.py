from pathlib import Path
from typing import Dict

import pytest
from src.omop_etl_wrapper.model.etl_stats import EtlStats
from src.omop_etl_wrapper.model.source_data import SourceData
from src.omop_etl_wrapper.util.io import read_yaml_file


@pytest.fixture
def source_config(source_config_dir: Path, source_data_test_dir: Path) -> Dict:
    source_config_path = source_config_dir / 'source_config.yml'
    source_config = read_yaml_file(source_config_path)
    # Insert the resolved path of the source_data_dir in the config
    source_data_dir = Path(source_data_test_dir / 'test_dir1').resolve()
    source_config['source_data_folder'] = source_data_dir
    return source_config


@pytest.fixture
def source_data1(source_config: Dict) -> SourceData:
    """Return SourceData instance with test_dir1 as source_dir."""
    return SourceData(source_config, EtlStats())


def test_source_folder_must_exist(source_config: Dict):
    source_config['source_data_folder'] = '/a/bad/path/that/should/not/exist'
    with pytest.raises(ValueError) as excinfo:
        SourceData(source_config, EtlStats())
    assert 'not a valid directory' in str(excinfo.value)


def test_get_file_returns_the_file(source_data1: SourceData):
    file1 = source_data1.get_source_file('source_file1.csv')
    assert file1.path.name == 'source_file1.csv'


def test_get_non_existing_file_raises_exception(source_data1: SourceData):
    with pytest.raises(FileNotFoundError):
        source_data1.get_source_file('source_file99.csv')


def test_source_data_counts_are_collected(source_config: Dict):
    source_config['count_source_rows'] = True
    source_data = SourceData(source_config, EtlStats())
    assert len(source_data._etl_stats.sources) == 3
