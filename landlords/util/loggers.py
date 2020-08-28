import coloredlogs
import logging

FORMATTER = "%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s"


def init_root_logger():
    logging.getLogger()
    coloredlogs.install(level="DEBUG", fmt=FORMATTER)


