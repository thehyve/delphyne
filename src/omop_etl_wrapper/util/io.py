import hashlib
from pathlib import Path
from typing import List, Set, Dict, Optional, Union

import yaml


def read_yaml_file(path: Path) -> Dict:
    with path.open('rt') as f:
        contents = yaml.safe_load(f.read())
    return contents


def write_yaml_file(contents: Dict, out_path: Path) -> None:
    with out_path.open('w') as out:
        yaml.dump(contents, out)


def is_hidden(path: Path) -> bool:
    """Return True if a path is hidden."""
    return path.name.startswith(('.', '~'))


def get_all_files_in_dir(directory: Path) -> List[Path]:
    """Return all but hidden files"""
    return [f for f in directory.glob('*')
            if f.is_file() and not is_hidden(f)]


def get_file_prefix(path: Path, suffix: str) -> Optional[str]:
    stem = path.stem
    if stem.endswith('_' + suffix):
        prefix = stem.rsplit('_' + suffix, 1)[0]
        return prefix
    return None


def file_has_valid_prefix(path: Path, suffix: str,
                          all_prefixes: Union[List[str], Set[str]],
                          valid_prefixes: Union[List[str], Set[str]]
                          ) -> bool:
    """
    Filter file names according to specific prefix rules.

    Files should be kept (returns True) when prefix:
    - matches a given list of prefixes to process
    - is not recognized (file name has no special meaning)
    - is None (filename consists of suffix only)
    If the prefix is recognized but not in the list to parse,
    the file should be ignored (returns False).
    valid_prefixes should be a subset of all_prefixes.

    :param path: Path
    :param suffix: str
    :param all_prefixes: Union[List[str], Set[str]]
    :param valid_prefixes: Union[List[str], Set[str]]

    :return: bool
    """
    file_prefix = get_file_prefix(path, suffix)
    if file_prefix in valid_prefixes:
        return True
    if file_prefix not in all_prefixes:
        return True
    if file_prefix is None:
        return True
    return False


def get_file_line_count(file_path: Path, skip_header: bool = True) -> int:
    """
    Get the line count of a text (non-binary) file.

    :param file_path: Path
    :param skip_header: bool, default True
        If True, the first line is not added to the line count.
    :return: int
    """
    if file_path.stat().st_size == 0:  # Empty file
        return 0
    n_rows = 0
    with file_path.open('rb') as f:
        if skip_header:
            next(f)
        for _ in f:
            n_rows += 1
    return n_rows


def get_file_checksum(path: Path) -> str:
    hash_md5 = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
