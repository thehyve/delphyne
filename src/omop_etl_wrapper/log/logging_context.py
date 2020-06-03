from logging import Logger, Formatter
from copy import copy


class LoggingFormatContext:
    """
    Use in a 'with' statement to temporarily use a different logging format.
    :param logger: Logger
        the default logger instance that will temporarily be modified
    :param new_formatter: Formatter
        logging Formatter instance that will be applied to the handlers in the logger
    """
    def __init__(self, logger: Logger, new_formatter: Formatter):
        self.logger = logger
        self.new_formatter = new_formatter
        self.old_handlers = [copy(h) for h in logger.handlers]

    def __enter__(self):
        for handler in self.logger.handlers:
            handler.setFormatter(self.new_formatter)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.handlers = self.old_handlers
