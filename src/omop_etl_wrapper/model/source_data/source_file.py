import logging
from inspect import signature
from pathlib import Path
from typing import Dict, Optional, List, OrderedDict, Callable

import pandas as pd
from omop_etl_wrapper.util.io import get_file_line_count

logger = logging.getLogger(__name__)


_READ_CSV_PARAMS = signature(pd.read_csv).parameters
_READ_SAS_PARAMS = signature(pd.read_sas).parameters


class SourceFile:
    def __init__(self, path: Path, params: Dict):
        self._path = path
        self._params = params

        # Cached data
        self._df: Optional[pd.DataFrame] = None
        self._csv: List[OrderedDict] = []

    def __repr__(self):
        return f'path={self.path}\n' + 'parameters='+str(self._params)

    @property
    def path(self) -> Path:
        return self._path

    def _remove_cached_df(self) -> None:
        if self._df is not None:
            logger.info(f'Removing cached df of {self.path.name}')
            self._df = None

    def _retrieve_cached_df(self):
        logger.info(f'Retrieving {self._path.name} DataFrame from cache')
        return self._df

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

    def _cache_df_copy(self, df: pd.DataFrame):
        logger.info('Caching deepcopy of DataFrame')
        self._df = df.copy(deep=True)

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
                ):
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

    def _read_csv_as_df(self, apply_dtypes: bool, **kwargs):
        config_kwargs = {kw: self._params.get(kw) for kw in self._params
                         if kw in _READ_CSV_PARAMS}
        full_kwargs = {**config_kwargs, **kwargs}
        if full_kwargs.get('delimiter') is None or full_kwargs.get('encoding') is None:
            raise ValueError(f'Cannot read {self._path.name} as DataFrame without providing '
                             f'both the encoding and a delimiter')
        df = pd.read_csv(self._path, dtype='object', **full_kwargs)
        if apply_dtypes:
            df = self._apply_dtypes(df)
        return df

    def _read_sas_as_df(self, apply_dtypes: bool, **kwargs):
        config_kwargs = {kw: self._params.get(kw) for kw in self._params
                         if kw in _READ_SAS_PARAMS}
        full_kwargs = {**config_kwargs, **kwargs}
        if full_kwargs.get('encoding') is None:
            raise ValueError(f'Cannot read {self._path.name} as DataFrame without providing '
                             f'the encoding')
        df = pd.read_sas(self._path, **full_kwargs)
        if apply_dtypes:
            df = self._apply_dtypes(df)
        return df

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
