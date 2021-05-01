from pathlib import Path
import functools
import logging
import logging.config

import toml

LOGGING_CONF_FILE = Path(__file__).parent / "logging_conf.toml"
logging.config.dictConfig(toml.load(LOGGING_CONF_FILE))


def logdec(func):
    @functools.wraps(func)
    def log(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        print(func.__module__)
        result = func(*args, **kwargs)
        return result

    return log
