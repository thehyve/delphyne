from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict

from .source_file import SourceFile
from ...config.models import SourceConfig
from ...model.etl_stats import EtlSource, etl_stats
from ...util import io

logger = logging.getLogger(__name__)


class SourceData:
    """Handle for all interactions related to source data files."""
    def __init__(self, config: Dict):
        self.source_config: SourceConfig = SourceConfig(**config)
        self._source_dir = self.source_config.source_data_folder
        self._file_defaults: Dict = self.source_config.file_defaults
        self._source_files: Dict[str, SourceFile] = self._collect_source_files()

        if self.source_config.count_source_rows:
            self._calculate_file_line_counts()

    @property
    def source_dir(self) -> Path:
        return self._source_dir

    def _collect_source_files(self) -> Dict[str, SourceFile]:
        source_files = self._source_dir.glob('*')
        source_files = [f for f in source_files if f.is_file() and not io.is_hidden(f)]

        source_file_dict = dict()
        file_config = self.source_config.source_files or {}
        for f in source_files:
            # Merge the default params with the file specific params
            params = {**self._file_defaults, **file_config.get(f.name, {})}
            source_file_dict[f.name] = SourceFile(path=f, params=params)
        return source_file_dict

    def get_source_file(self, file_name: str) -> SourceFile:
        source_file = self._source_files.get(file_name)
        if source_file is None:
            raise FileNotFoundError(f'Could not find source file {file_name} in source folder')
        return source_file

    def _calculate_file_line_counts(self) -> None:
        logger.info('Collecting source file row counts')
        for file_name, source_file in self._source_files.items():
            start_time = datetime.now()
            row_count = source_file.get_line_count()
            end_time = datetime.now()
            etl_source = EtlSource(start_time, end_time, file_name, row_count)
            etl_stats.add_source(etl_source)
