import logging
from os.path import join, dirname, realpath

GLOBAL_LOGGING_LEVEL = None
dir_path = dirname(realpath(__file__))


def get_logger(name=__name__, level=logging.INFO):
    if GLOBAL_LOGGING_LEVEL:
        level = GLOBAL_LOGGING_LEVEL
    logging.basicConfig(level=level, filename=join(dir_path, "debug.log"))
    logger = logging.getLogger(name)
    return logger
