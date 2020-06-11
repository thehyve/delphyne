import hashlib
from pathlib import Path
from typing import Dict

import yaml


def read_yaml_file(path: Path) -> dict:
    with path.open('rt') as f:
        contents = yaml.safe_load(f.read())
    return contents


def write_yaml_file(contents: Dict, out_path: Path) -> None:
    with out_path.open('w') as out:
        yaml.dump(contents, out)


def get_file_line_count(file_path: Path, skip_header: bool = True) -> int:
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
