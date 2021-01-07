import logging
from pathlib import Path
from typing import Optional, Dict, Any

from pydantic import BaseModel, validator, DirectoryPath
from ...util.io import is_hidden

logger = logging.getLogger(__name__)


class SourceConfig(BaseModel):
    source_data_folder: DirectoryPath
    count_source_rows: bool
    file_defaults: Optional[Dict[str, Any]]
    source_files: Optional[Dict[str, Dict]]

    @validator('source_files')
    def check_source_files_present(cls,
                                   config_source_files: Optional[Dict[str, Dict]],
                                   values: Dict,
                                   ) -> Optional[Dict[str, Dict]]:
        if config_source_files is None or 'source_data_folder' not in values:
            return config_source_files
        source_dir: Path = values['source_data_folder']
        actual_source_files = {f.name for f in source_dir.glob('*')
                               if f.is_file() and not is_hidden(f)}
        if not actual_source_files:
            logger.warning(f'No source files were found at: {source_dir}')
        for f in config_source_files.keys():
            if f not in actual_source_files:
                logger.warning(f'Source file "{f}" not found in source folder: {source_dir}')
        return config_source_files
