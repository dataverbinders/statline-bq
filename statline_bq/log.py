# Logging module, reading a logging configuration toml file, defining the logs output folder and creating a logging decorator that can be applied to any function.
from pathlib import Path
import functools
import logging
import logging.config

import toml

LOG_DIR = Path.home() / ".statline-bq/logs"
LOGGING_CONF_FILE = Path(__file__).parent / "logging_conf.toml"

log_conf = toml.load(LOGGING_CONF_FILE)
for handler, handler_conf in log_conf.get("handlers").items():
    if "File" in handler_conf.get("class"):
        filepath = LOG_DIR / handler_conf.get("filename")
        handler_conf["filename"] = str(filepath)

if not (LOG_DIR.exists() and LOG_DIR.is_dir()):
    LOG_DIR.mkdir(parents=True)

logging.config.dictConfig(log_conf)


def logdec(func):
    """A logging decorator wrapping a function with a standard logging mechanism.

    This decorator wraps a function with a standard logging mechanism, providing the following functionalities:
    * Logs the calling of the function and the parameters passed to it when called.
    * If no exception is raised during the function run - returns the result and logs the succesful completion (not the result).
    * If an exception is raised - logs the exception and returns None.

    Based on https://medium.com/swlh/add-log-decorators-to-your-python-project-84094f832181

    Parameters
    ----------
    func : the function to be wrapped

    Returns
    -------
    result or None
        The result of the function if no exception was raised, None otherwise
    """

    @functools.wraps(func)
    def logged(*args, **kwargs):
        # Create a list of the positional arguments passed to function
        args_passed_in_function = [repr(a) for a in args]
        # Create a list of the keyword arguments
        kwargs_passed_in_function = [f"{k}={v!r}" for k, v in kwargs.items()]
        # The lists of positional and keyword arguments is joined together to form final string
        formatted_arguments = ", ".join(
            args_passed_in_function + kwargs_passed_in_function
        )
        logger = logging.getLogger(func.__module__)
        logger.info(
            f"Starting function {func.__name__} with arguments: ({formatted_arguments})"
        )
        try:
            result = func(*args, **kwargs)
            logger.info(f"Succesfully finished function {func.__name__}")
            return result
        except Exception as e:
            # Log exception if occurs in function
            logger.exception(f"Exception in {func.__name__}: {e}")
            raise e

    return logged
