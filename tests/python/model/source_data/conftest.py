from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def source_config_dir(test_data_dir: Path) -> Path:
    return test_data_dir / 'source_data_configs'


@pytest.fixture(scope="session")
def source_data_test_dir(test_data_dir: Path) -> Path:
    """Directory holding subdirectories with source data test files."""
    return test_data_dir / 'source_data_files'
