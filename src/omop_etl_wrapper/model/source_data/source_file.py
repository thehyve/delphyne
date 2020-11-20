import csv
import logging
from copy import deepcopy
from inspect import signature
from pathlib import Path
from typing import Dict, Optional, List, OrderedDict, Callable, Generator

import pandas as pd

from ...util.io import get_file_line_count

logger = logging.getLogger(__name__)


_READ_CSV_PARAMS = signature(pd.read_csv).parameters
_READ_SAS_PARAMS = signature(pd.read_sas).parameters

_CSV_DIALECT_PARAMS = {kw for kw in dir(csv.Dialect)
                       if not kw.startswith('_')}
_CSV_DICT_READER_PARAMS = {'fieldnames', 'restkey', 'restval', 'dialect'}
_FULL_CSV_PARAMS = _CSV_DIALECT_PARAMS.union(_CSV_DICT_READER_PARAMS)


class SourceFile:
    def __init__(self, path: Path, params: Dict):
        self._path = path
        self._params = params

        # Cached data
        self._df: Optional[pd.DataFrame] = None
        self._csv: List[OrderedDict] = []

    def __repr__(self):
        return f'path={self.path}\n' + 'parameters=' + str(self._params)

    @property
    def path(self) -> Path:
        return self._path

    @property
    def config(self) -> Dict:
        """Read-only copy of config parameters."""
        return deepcopy(self._params)

    @property
    def dtypes(self) -> Dict:
        """Read-only copy of specified dtypes."""
        return deepcopy(self._params.get('dtypes', {}))

    def _remove_cached_df(self) -> None:
        if self._df is not None:
            logger.info(f'Removing cached df of {self.path.name}')
            self._df = None

    def _retrieve_cached_df(self) -> pd.DataFrame:
        logger.info(f'Retrieving {self._path.name} DataFrame from cache')
        return self._df

    def _cache_df_copy(self, df: pd.DataFrame) -> None:
        logger.info('Caching deepcopy of DataFrame')
        self._df = df.copy(deep=True)

    def _remove_cached_csv(self) -> None:
        if self._csv:
            logger.info(f'Removing cached csv records of {self.path.name}')
            self._csv = []

    def _retrieve_cached_csv(self) -> List[OrderedDict]:
        logger.info(f'Retrieving {self._path.name} csv records from cache')
        return self._csv

    def _cache_csv_copy(self, csv_records: List[OrderedDict]) -> None:
        logger.info('Caching deepcopy of csv records')
        self._csv = deepcopy(csv_records)

    def get_csv_as_df(self,
                      apply_dtypes: bool,
                      force_reload: bool = False,
                      cache: bool = False,
                      **kwargs
                      ) -> pd.DataFrame:
        """
        Return a delimited text file as a pandas DataFrame.

        :param apply_dtypes: bool
            Apply source_config dtypes to the columns in the DataFrame.
            If False, all columns will be loaded as 'object' dtype.
        :param force_reload: bool, default False
            If True, remove the cached df (if there is one), and reload
            from source file.
        :param cache: bool, default False
            If True, keep the returned df in memory for future use.
        :param kwargs:
            Additional keyword arguments are passed on directly to
            pandas read_csv method.

        :return: pd.DataFrame
        """
        return self._get_df(read_func=self._read_csv_as_df, apply_dtypes=apply_dtypes,
                            force_reload=force_reload, cache=cache, **kwargs)

    def get_sas_as_df(self,
                      apply_dtypes: bool,
                      force_reload: bool = False,
                      cache: bool = False,
                      **kwargs
                      ) -> pd.DataFrame:
        """
        Read a SAS source file and return as pandas DataFrame.

        :param apply_dtypes: bool
            Apply source_config dtypes to the columns in the DataFrame.
            If False, dtypes will stay as specified in the SAS file.
        :param force_reload: bool, default False
            If True, remove the cached df (if there is one), and reload
            from source file.
        :param cache: bool, default False
            If True, keep the returned df in memory for future use.
        :param kwargs:
            Additional keyword arguments are passed on directly to
            pandas read_sas method.
        :return: pd.DataFrame
        """
        return self._get_df(read_func=self._read_sas_as_df, apply_dtypes=apply_dtypes,
                            force_reload=force_reload, cache=cache, **kwargs)

    def cache_df(self, df: pd.DataFrame) -> None:
        """
        Save a DataFrame in memory for future use.

        The main reason for manually caching a DataFrame linked to a
        source file, would be if it has already undergone a kind of
        generic processing that then doesn't need to be redone when
        using it in a later transformation.
        Calling this method will overwrite any existing cached DataFrame
        for this source file.

        :param df: pd.DataFrame
            The DataFrame to save in memory.
        :return: None
        """
        self._cache_df_copy(df)

    def _get_df(self,
                read_func: Callable,
                apply_dtypes: bool,
                force_reload: bool,
                cache: bool,
                **kwargs
                ) -> pd.DataFrame:
        if force_reload:
            self._remove_cached_df()

        if self._df is not None:
            df = self._retrieve_cached_df()
        else:
            logger.info(f'Reading {self._path.name} as DataFrame')
            df = read_func(apply_dtypes, **kwargs)

        if cache:
            self._cache_df_copy(df)
        else:
            self._remove_cached_df()

        return df

    def _read_csv_as_df(self, apply_dtypes: bool, **kwargs) -> pd.DataFrame:
        config_kwargs = {kw: self._params.get(kw) for kw in self._params
                         if kw in _READ_CSV_PARAMS}
        full_kwargs = {**config_kwargs, **kwargs}
        self._check_missing_params(params=full_kwargs, required=['delimiter', 'encoding'])
        df = pd.read_csv(self._path, dtype='object', **full_kwargs)
        if apply_dtypes:
            df = self._apply_dtypes(df)
        return df

    def _read_sas_as_df(self, apply_dtypes: bool, **kwargs) -> pd.DataFrame:
        config_kwargs = {kw: self._params.get(kw) for kw in self._params
                         if kw in _READ_SAS_PARAMS}
        full_kwargs = {**config_kwargs, **kwargs}
        self._check_missing_params(params=full_kwargs, required=['encoding'])
        df = pd.read_sas(self._path, **full_kwargs)
        if apply_dtypes:
            df = self._apply_dtypes(df)
        return df

    def _check_missing_params(self, params: Dict, required: List[str]) -> None:
        missing = [kw for kw in required if params.get(kw) is None]
        if missing:
            raise ValueError(f'Cannot read {self._path.name}, missing required '
                             f'parameters: {", ".join(missing)}')

    def _apply_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        # Apply source_config dtypes to the columns in the DataFrame.
        dtypes = self._params.get('dtypes', {})
        if not dtypes:
            logger.warning(f'No dtypes were found in source config for {self._path}')
            return df
        logger.info('Applying dtypes')
        # The object dtype cannot be directly converted to Int64, so we
        # first convert to float64
        int_cols = [col for col, dtype in dtypes.items() if dtype.startswith('Int')]
        df[int_cols] = df[int_cols].astype('float64')
        return df.astype(dtypes)

    def get_line_count(self) -> Optional[int]:
        """
        Get the line count of the file (excluding header).

        Only if the file is explicitly specified as not binary in the
        source_config, will the line count be calculated.

        :return: int or None
        """
        is_binary = self._params.get('binary')
        if is_binary is False:
            try:
                logger.debug(f'Reading value separated file {self.path.name}')
                n_rows = get_file_line_count(self.path)
            except Exception as e:
                logger.error(f'Could not read contents of source file: {self.path.name}')
                logger.error(e)
            else:
                logger.info(f'{n_rows} data rows were counted in {self.path.name}')
                return n_rows
        elif is_binary is not True:
            logger.warning(f'No binary field available for for file {self.path.name} '
                           f'in the source config. Skipping line count calculation.')
        return None

    def get_csv_as_generator_of_dicts(self, **kwargs) -> Generator[OrderedDict, None, None]:
        """
        Return delimited text file as a generator of OrderedDicts.

        Reading in this way keeps memory use very low, but you can only
        access one row at a time.

        :param kwargs:
            Additional keyword arguments are passed on directly to
            the csv module's DictReader.
        :return: OrderedDict generator
        """
        logger.info(f'Reading {self._path.name} as csv records')
        full_kwargs = {**self._params, **kwargs}
        self._check_missing_params(params=full_kwargs, required=['delimiter', 'encoding'])
        dict_reader_params = {kw: full_kwargs.get(kw) for kw in full_kwargs
                              if kw in _FULL_CSV_PARAMS}

        with self.path.open('r', encoding=full_kwargs['encoding']) as f:
            reader = csv.DictReader(f, **dict_reader_params)
            for row in reader:
                yield row

    def get_csv_as_list_of_dicts(self,
                                 cache: bool = False,
                                 **kwargs
                                 ) -> List[OrderedDict]:
        """
        Return a delimited text file as a list of OrderedDicts.

        This method loads the entire file into memory, meaning you can
        access all rows in any order.

        :param cache:
            If True, keep the full list of dictionaries in memory for
            future use.
        :param kwargs:
            Additional keyword arguments are passed on directly to
            the csv module's DictReader.
        :return: List[OrderedDict]
        """
        if self._csv:
            csv_records = self._retrieve_cached_csv()
        else:
            logger.info(f'Reading {self._path.name} as csv records')
            full_kwargs = {**self._params, **kwargs}
            self._check_missing_params(params=full_kwargs, required=['delimiter', 'encoding'])
            dict_reader_params = {kw: full_kwargs.get(kw) for kw in full_kwargs
                                  if kw in _FULL_CSV_PARAMS}

            with self.path.open('r', encoding=full_kwargs['encoding']) as f:
                reader = csv.DictReader(f, **dict_reader_params)
                csv_records = [row for row in reader]

        if cache:
            self._cache_csv_copy(csv_records)
        else:
            self._remove_cached_csv()

        return csv_records
