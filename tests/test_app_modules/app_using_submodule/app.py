import logging

from tests.test_app_modules.app_using_submodule.submodule import do_something


logger = logging.getLogger(__name__)


def run(analysis):
    logger.info("Log message from app.")
    do_something()
