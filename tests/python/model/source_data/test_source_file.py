import logging
from pathlib import Path
from typing import Dict
from unittest.mock import MagicMock

import pandas as pd
import pytest
from numpy import nan, dtype
from src.omop_etl_wrapper.model.source_data import SourceFile


def get_file_params(**kwargs) -> Dict:
    """Get a file params dictionary.

    Via kwargs, default parameters can be overwritten, or additional
    parameters can be added.
    """
    default_params = {
        'delimiter': "\t",
        'encoding': 'utf-8',
        'quotechar': '"',
        'binary': False
    }
    file_params = {**default_params, **kwargs}
    return file_params


@pytest.fixture
def source_file1_df(source_data_test_dir: Path) -> pd.DataFrame:
    """Get DataFrame of source_file1.csv without setting dtypes."""
    file_path = source_data_test_dir / 'test_dir1' / 'source_file1.csv'
    params = get_file_params(delimiter=',')
    source_file = SourceFile(file_path, params)
    return source_file.get_csv_as_df(apply_dtypes=False)


@pytest.fixture
def source_file2(source_data_test_dir: Path) -> SourceFile:
    """Get SourceFile instance of source_file2.tsv with dtypes."""
    file_path = source_data_test_dir / 'test_dir1' / 'source_file2.tsv'
    dtypes = {
        'column_a': 'object',
        'column_b': 'Int64',
        'column_c': 'datetime64[ns]',
        'column_d': 'float64',
    }
    params = get_file_params(dtypes=dtypes)
    return SourceFile(file_path, params)


@pytest.fixture
def sas_source_file(source_data_test_dir: Path) -> SourceFile:
    """Get DataFrame of beer.sas7bdat."""
    file_path = source_data_test_dir / 'test_dir1' / 'beer.sas7bdat'
    params = get_file_params(binary=True)
    return SourceFile(file_path, params)


def test_source_file1_df_contents(source_file1_df: pd.DataFrame):
    expected = {
        'A': {0: 'a', 1: 'b,b', 2: "c'd"},
        'B': {0: nan, 1: '2', 2: '3'},
        'C': {0: nan, 1: '11.8', 2: '12.2'}
    }
    assert source_file1_df.to_dict() == expected


def test_source_file2_has_only_object_dtypes(source_file2: SourceFile):
    """When apply_dtypes=False, all columns should have object dtype."""
    df = source_file2.get_csv_as_df(apply_dtypes=False)
    assert list(df.dtypes) == [dtype('O')] * 4


def test_get_csv_as_df_requires_delimiter(source_file2: SourceFile):
    del source_file2._params['delimiter']
    with pytest.raises(ValueError) as excinfo:
        source_file2.get_csv_as_df(apply_dtypes=False)
    assert "delimiter" in str(excinfo.value)


def test_get_csv_as_df_requires_encoding(source_file2: SourceFile):
    del source_file2._params['encoding']
    with pytest.raises(ValueError) as excinfo:
        source_file2.get_csv_as_df(apply_dtypes=False)
    assert "encoding" in str(excinfo.value)


def test_source_file2_has_config_dtypes(source_file2: SourceFile):
    df = source_file2.get_csv_as_df(apply_dtypes=True)
    expected_dtypes = {
        'column_a': dtype('O'),
        'column_b': pd.Int64Dtype(),
        'column_c': dtype('<M8[ns]'),
        'column_d': dtype('float64'),
    }
    assert df.dtypes.to_dict() == expected_dtypes


def test_cache_method_is_called(source_file2: SourceFile):
    source_file2._cache_df_copy = MagicMock()
    source_file2.get_csv_as_df(apply_dtypes=False, cache=True)
    source_file2._cache_df_copy.assert_called_once()


def test_get_csv_with_a_cache_does_not_reload_file(source_file2: SourceFile):
    source_file2.get_csv_as_df(apply_dtypes=False, cache=True)
    source_file2._retrieve_cached_df = MagicMock()
    source_file2._read_csv_as_df = MagicMock()
    source_file2.get_csv_as_df(apply_dtypes=False, cache=True)
    source_file2._retrieve_cached_df.assert_called_once()
    source_file2._read_csv_as_df.assert_not_called()


def test_force_reload_ignores_cache(source_file2: SourceFile):
    source_file2.get_csv_as_df(apply_dtypes=False, cache=True)
    source_file2._retrieve_cached_df = MagicMock()
    source_file2._read_csv_as_df = MagicMock()
    source_file2.get_csv_as_df(apply_dtypes=False, cache=True, force_reload=True)
    source_file2._retrieve_cached_df.assert_not_called()
    source_file2._read_csv_as_df.assert_called_once()


def test_modifying_returned_df_does_not_affect_cached_df(source_file2: SourceFile):
    df = source_file2.get_csv_as_df(apply_dtypes=False, cache=True)
    df.drop(labels='column_a', axis=1, inplace=True)
    assert 'column_a' not in df.columns
    df = source_file2.get_csv_as_df(apply_dtypes=False, cache=True)
    assert 'column_a' in df.columns


def test_setting_cached_df_manually(source_file2: SourceFile):
    df = source_file2.get_csv_as_df(apply_dtypes=False, cache=False)
    df.drop(labels='column_a', axis=1, inplace=True)
    source_file2.cache_df(df)
    df = source_file2.get_csv_as_df(apply_dtypes=False, cache=False)
    assert 'column_a' not in df.columns


def test_sas_requires_encoding_param(sas_source_file: SourceFile):
    del sas_source_file._params['encoding']
    with pytest.raises(ValueError) as excinfo:
        sas_source_file.get_sas_as_df(apply_dtypes=False)
    assert "encoding" in str(excinfo.value)


def test_kwargs_overrule_file_params(sas_source_file: SourceFile):
    """Adding encoding='iso8859' as a kwarg to get_sas_as_df, should
    overrule the original encoding value in the SourceFile's params."""
    assert sas_source_file._params['encoding'] == 'utf-8'
    with pytest.raises(UnicodeDecodeError):
        sas_source_file.get_sas_as_df(apply_dtypes=False)
    df = sas_source_file.get_sas_as_df(apply_dtypes=False, encoding='iso8859')
    assert df.shape == (30, 5)


def test_invalid_kwarg_raises_error(source_file2: SourceFile):
    with pytest.raises(TypeError) as excinfo:
        source_file2.get_csv_as_df(apply_dtypes=False, bad_kwarg=42)
    assert "unexpected keyword argument 'bad_kwarg'" in str(excinfo.value)


def test_get_line_count(source_file2: SourceFile):
    assert source_file2.get_line_count() == 4


def test_binary_file_wont_return_line_count(sas_source_file: SourceFile):
    assert sas_source_file.get_line_count() is None


def test_absent_binary_flag(caplog, sas_source_file: SourceFile):
    """When a file has no information on whether it is binary, the
    returned line count should be None, and a warning logged."""
    del sas_source_file._params['binary']
    with caplog.at_level(logging.DEBUG):
        assert sas_source_file.get_line_count() is None
    assert 'No binary field available for for file beer.sas7bdat' in caplog.text


def test_read_csv_types(source_file2: SourceFile):
    rows = source_file2.get_csv_as_list_of_dicts()
    assert type(rows) == list
    # csv returns OrderedDict <= 3.7, regular dict for >= 3.8
    assert all([issubclass(type(row), dict) for row in rows])


def test_read_csv_no_cache(source_file2: SourceFile):
    source_file2.get_csv_as_list_of_dicts()
    assert not source_file2._csv


def test_read_csv_cache(source_file2: SourceFile):
    source_file2.get_csv_as_list_of_dicts(cache=True)
    assert len(source_file2._csv) == 4


def test_read_csv_generator_not_subscriptable(source_file2: SourceFile):
    rows = source_file2.get_csv_as_generator_of_dicts()
    with pytest.raises(TypeError) as excinfo:
        second_row = rows[1]
    assert "'generator' object is not subscriptable" in str(excinfo.value)


def test_read_csv_values_accessible_via_column_names(source_file2: SourceFile):
    rows = source_file2.get_csv_as_generator_of_dicts()
    first_row = next(rows)
    assert first_row['column_a'] == 'Tungsten carbide'


def test_read_csv_additional_kwargs(source_file2: SourceFile):
    assert source_file2.get_csv_as_list_of_dicts(strict=True)[0]
