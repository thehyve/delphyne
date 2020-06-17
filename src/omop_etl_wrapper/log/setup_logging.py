import logging.config
import time

from .._paths import LOG_OUTPUT_DIR, LOG_CONFIG_PATH
from ..util.io import read_yaml_file


def setup_logging(debug: bool) -> None:
    """
    Setup logging configuration.

    :param debug: bool
        If True, debug messages will be displayed
    """
    log_dir = LOG_OUTPUT_DIR
    log_dir.mkdir(exist_ok=True)
    time_string = time.strftime('%Y-%m-%dT%H%M%S')
    logfile = log_dir.joinpath(f'{time_string}.log')

    default_level = logging.DEBUG if debug else logging.INFO
    config_path = LOG_CONFIG_PATH
    if config_path.exists():
        config = read_yaml_file(config_path)
        config['root']['level'] = default_level
        config['handlers']['file']['filename'] = logfile
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
