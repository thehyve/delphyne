# Copyright 2019 The Hyve
#
# Licensed under the GNU General Public License, version 3,
# or (at your option) any later version (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.gnu.org/licenses/
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from numpy import nan

from .etl_stats import EtlSource
from ..util import io

logger = logging.getLogger(__name__)


_ARHCIVED_SOURCE_COUNTS_FILE = Path('./.archived_source_counts.yaml')


class SourceDataException(Exception):
    pass


class SourceData:
    """Handle for all interactions related to source data files."""
    def __init__(self, config: Dict):
        pass
        # TODO: implement init using config parameters
        self.source_dir = Path(config['source_data_folder'])
        self.file_defaults: dict = config['file_defaults']
        self.source_files: List[SourceFile] = self.get_source_files()
        # self.source_counts: Dict[str, int] = self._get_archived_source_counts()
        # self.source_config: Dict = io.read_yaml_file(source_config)

    @staticmethod
    def _get_archived_source_counts() -> Dict[str, int]:
        if _ARHCIVED_SOURCE_COUNTS_FILE.exists():
            return io.read_yaml_file(_ARHCIVED_SOURCE_COUNTS_FILE)
        else:
            return dict()

    def load_sas_file(self,
                      table_name: str,
                      apply_dtypes: bool = True,
                      ) -> pd.DataFrame:
        """
        Read a SAS source file and return as pandas DataFrame.

        :param table_name: str
            file name in source data folder (without suffix)
        :param apply_dtypes: bool, default True
            convert the df columns to the dtypes specified in
            data_types.yaml. If False, dtypes will stay as specified in
            the SAS file
        :return: pd.DataFrame
        """
        logger.info(f'Reading source file: {table_name}')
        source_file_path = self._find_source_file(table_name)
        file_encoding = self._get_file_encoding(table_name)
        df = pd.read_sas(source_file_path, encoding=file_encoding)

        if apply_dtypes:
            dtypes_dict = self._get_file_dtypes(table_name)
            df = SourceData.apply_dtypes(df, dtypes_dict)
        logger.info(f'Source file: {table_name} read successfully')
        return df

    def _find_source_file(self, source_name: str) -> Path:
        sas_files = [f for f in self.source_dir.glob('*') if f.suffix in {'.sas7bdat', '.xpt'}]
        matching_files = [f for f in sas_files if f.stem == source_name]
        if not matching_files:
            raise SourceDataException(f'No SAS files found that match the name "{source_name}"')
        elif len(matching_files) > 1:
            raise SourceDataException(f'Multiple SAS files found that match the '
                                      f'name "{source_name}": {matching_files}')
        else:
            return matching_files[0]

    def _get_file_encoding(self, table_name: str) -> str:
        try:
            return self.source_config[table_name]['encoding']
        except KeyError:
            raise SourceDataException(f'No encoding found in source_config for table: "{table_name}"')

    def _get_file_dtypes(self, table_name: str) -> Dict[str, str]:
        try:
            return self.source_config[table_name]['dtypes']
        except KeyError:
            raise SourceDataException(f'No dtypes found in source_config for table: "{table_name}"')

    @staticmethod
    def apply_dtypes(df: pd.DataFrame, dtypes: dict) -> pd.DataFrame:
        """
        :param df: pandas DataFrame
            The df to which dtypes will be applied
        :param dtypes: dict
            Dictionary with the dtypes that will be set for the columns
        :return df: pandas DataFrame
        """
        # Replace empty strings with numpy.nan for all non-string columns
        non_string_cols = [col for col, dtype in dtypes.items() if dtype != 'object']
        df[non_string_cols] = df[non_string_cols].replace('', nan)
        # str cannot be directly converted to Int64, so we first convert to float64
        int_cols = [col for col, dtype in dtypes.items() if dtype == 'Int64']
        df[int_cols] = df[int_cols].astype('float64')
        return df.astype(dtypes)

    def get_source_counts(self) -> List[EtlSource]:
        logger.info('Collecting source file row counts')
        etl_sources = []
        for path in self.source_dir.glob('*'):
            if path.name.startswith(('.', '~')):
                logger.debug(f'Skipping file: {path}')
                continue
            file_checksum = io.get_file_checksum(path)
            archived_source_count = self.source_counts.get(file_checksum)
            if archived_source_count is not None:
                logger.debug(f'Collected {archived_source_count} row count from archive for {path.name}')
                etl_sources.append(EtlSource(source_name=path.name, n_rows=archived_source_count))
            else:
                start_time = datetime.now()
                row_count = self.calculate_table_row_count(path)
                end_time = datetime.now()
                etl_sources.append(EtlSource(start_time, end_time, path.name, row_count))
                self.source_counts[file_checksum] = row_count
        return etl_sources

    @staticmethod
    def calculate_table_row_count(file_path: Path) -> Optional[int]:
        """
        Count the rows of the given source table (excluding header).

        :param file_path: Path
            The full path of the source table
        :return row count: int
        """
        try:
            # Read SAS files with pandas
            if file_path.suffix.lower() in {'.xpt', '.sas7bdat'}:
                logger.debug(f'Reading SAS file {file_path}')
                df = pd.read_sas(file_path)
                n_rows = len(df.index)
            # Otherwise assume regular value separated file
            else:
                logger.debug(f'Reading value separated file {file_path}')
                n_rows = io.get_file_line_count(file_path)
        except Exception as e:
            logger.error(f'Could not read contents of source file: {file_path}')
            logger.error(e)
            return None
        logger.info(f'{n_rows} data rows were counted in {file_path.name}')
        return n_rows

    def write_source_count_archive(self) -> None:
        logger.info(f'Writing source row count archive file at {_ARHCIVED_SOURCE_COUNTS_FILE}')
        io.write_yaml_file(self.source_counts, _ARHCIVED_SOURCE_COUNTS_FILE)
