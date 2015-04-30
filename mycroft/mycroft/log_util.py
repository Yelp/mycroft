# -*- coding: utf-8 -*-
import sys
import traceback

import staticconf
from sherlock.common.loggers import PipelineStreamLogger

""" Helper to provide simple log() and log_exception() interfaces for
mycroft.
"""


class NoOpLogger(object):

    def write_msg(self, msg, error_msg=None):
        pass


logger = NoOpLogger()


def _get_logger(run_local, tag):
    try:
        return PipelineStreamLogger(
            staticconf.read_string("log_stream_name"),
            run_local, tag
        )
    except:
        logger.write_msg("Error creating a pipeline stream logger!")
        return logger  # Return existing logger instance in case of errors


def setup_pipeline_stream_logger(run_local, tag='mycroft'):
    """ Expected to be called once on startup. Idempotent.
    """
    global logger
    if isinstance(logger, NoOpLogger):
        logger = _get_logger(run_local, tag)


def log(msg):
    logger.write_msg(msg)


def log_debug(msg):
    # TODO: Introduce logging levels
    # Let us revisit this and fix in future - for now, this debug msg will
    # help in debugging.
    logger.write_msg(msg)


# only for tests
def reset_logger():
    global logger
    logger = NoOpLogger()


def log_exception(msg):
    """ Adds an exception trace to log in addition to printing msg.
    """
    exc_type, exc_value, exc_tb = sys.exc_info()
    error_info = {
        'crash_tb': ''.join(traceback.format_tb(exc_tb)),
        'crash_exc':
            traceback.format_exception_only(exc_type, exc_value)[0].strip(),

    }
    logger.write_msg(msg, error_msg=error_info)
