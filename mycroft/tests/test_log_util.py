# -*- coding: utf-8 -*-
import pytest

import staticconf.testing
from mycroft.log_util import NoOpLogger
from mycroft.log_util import logger
from mycroft.log_util import reset_logger
from mycroft.log_util import setup_pipeline_stream_logger
from sherlock.common.loggers import PipelineStreamLogger


@pytest.yield_fixture
def reset_logger_after_test():
    try:
        yield
    finally:
        reset_logger()


def test_default_logger():
    assert isinstance(logger, NoOpLogger)


def test_logger_is_PSL_after_setup(reset_logger_after_test):
    config = {
        'log_stream_name': 'mycroft-test',
        'run_local.logdir': 'logs/',
    }
    _ = reset_logger_after_test  # NOQA
    with staticconf.testing.MockConfiguration(config):
        setup_pipeline_stream_logger(True)
    # reimport to see the changes
    from mycroft.log_util import logger
    assert isinstance(logger, PipelineStreamLogger)


def test_logger_is_unchanged_if_exception():
    config = {
        'invalid_key': 'mycroft-test',
    }
    with staticconf.testing.MockConfiguration(config):
        setup_pipeline_stream_logger(True)
    # reimport to see the changes, if any
    from mycroft.log_util import logger
    assert isinstance(logger, NoOpLogger)
