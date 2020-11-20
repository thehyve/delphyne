import io
import logging
from typing import List

import pytest
from src.omop_etl_wrapper.log.log_formats import MESSAGE_ONLY
from src.omop_etl_wrapper.log.logging_context import LoggingFormatContext

logger = logging.getLogger('test_logger')
logger.setLevel(logging.DEBUG)


@pytest.fixture(scope='function')
def string_stream_handler() -> logging.StreamHandler:
    """Add a StringIO StreamHandler to logger and yield it.
    Afterwards remove all logger handlers."""
    string_io = io.StringIO()
    stream_handler = logging.StreamHandler(string_io)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    yield stream_handler
    logger.handlers = []


def get_stringio_lines(string_io: io.StringIO) -> List[str]:
    return string_io.getvalue().split('\n')


def test_logging_context(string_stream_handler: logging.StreamHandler, caplog):
    """logging_context allows temporarily changing the log format."""
    dinosaur_format = logging.Formatter("🦕%(message)s🦕")
    logger.info('this has regular format')
    with LoggingFormatContext(logger, dinosaur_format):
        logger.info('THIS HAS AWESOME DINOSAUR FORMAT!')
    logger.info('this has boring regular format again')
    log_lines = get_stringio_lines(string_stream_handler.stream)
    assert log_lines[0] == 'INFO - this has regular format'
    assert log_lines[1] == '🦕THIS HAS AWESOME DINOSAUR FORMAT!🦕'
    assert log_lines[2] == 'INFO - this has boring regular format again'


def test_log_format_message_only(caplog):
    message = 'back in my day logging was just about the message'
    logger.info('back in my day logging was just about the message')
    assert MESSAGE_ONLY.format(caplog.records[0]) == message
