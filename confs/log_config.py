import logging
from logging import config
import sys

class _ExcludeErrorsFilter(logging.Filter):
    def filter(self, record):
        """Only lets through log messages with log level below ERROR ."""
        return record.levelno < logging.ERROR


LOG_CONFIG = {
    'version': 1,
    'filters': {
        'exclude_errors': {
            '()': _ExcludeErrorsFilter
        }
    },
    'formatters': {
        # Modify log message format here or replace with your custom formatter class
        'basic_formatter': {
            'format': '[%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s',
            'datefmt':'%a, %d %b %Y %H:%M:%S'
        },
        'colored_formatter': {
            '()': 'colorlog.ColoredFormatter',  # colored output

            # --> %(log_color)s is very important, that's what colors the line
            'format': '%(log_color)s [%(asctime)s] %(levelname)s [%(filename)s.%(funcName)s:%(lineno)d] %(message)s',
            'datefmt':'%a, %d %b %Y %H:%M:%S'
        },

    },
    'handlers': {
        'console_stderr': {
            # Sends log messages with log level ERROR or higher to stderr
            'class': 'logging.StreamHandler',
            'level': 'ERROR',
            'formatter': 'colored_formatter',
            'stream': sys.stderr
        },
        'console_stdout': {
            # Sends log messages with log level lower than ERROR to stdout
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'colored_formatter',
            'filters': ['exclude_errors'],
            'stream': sys.stdout
        },
        'file': {
            # Sends all log messages to a file
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'formatter': 'basic_formatter',
            'filename': './logs/app.log',
            'encoding': 'utf8'
        }
    },
    'root': {
        # In general, this should be kept at 'NOTSET'.
        # Otherwise it would interfere with the log levels set for each handler.
        'level': 'NOTSET',
        'handlers': ['console_stderr', 'console_stdout', 'file']
    },
}