import logging
from logging.config import dictConfig


def get_logger(logger_name: str, log_level: str = 'INFO') -> logging.Logger:
    logger_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'format': '[%(filename)s:%(lineno)s - %(funcName)20s()] %(asctime)s %(message)s'
            },
        },
        'handlers': {
            'console': {
                'level': log_level,
                'formatter': 'default',
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stderr',
            },
            'file': {
                'level': log_level,
                'class': 'logging.handlers.RotatingFileHandler',
                'formatter': 'default',
                'filename': 'log_file.log',
                'maxBytes': 500000,
                'backupCount': 10
            }
        },
        'loggers': {
            '': {
                'handlers': ['file', 'console'],
                'level': log_level,
                'propagate': False
            },
        }
    }

    dictConfig(logger_config)

    return logging.getLogger(logger_name)


LOG_LEVEL = 'DEBUG'

logger = get_logger('root', LOG_LEVEL)
