# -*- coding: utf-8 -*-
"""
loggers.py intends to hold a collection of loggers
"""

import sys
from sherlock.common.pipeline import PipelineStreamLoggerBase


class PipelineStreamLogger(PipelineStreamLoggerBase):
    """
    PipelineStreamLogger enables setting up a log and writes to
    it, with some parameters commonly used for the sdw pipeline

    """

    def write_msg(self, status, job_start_secs=None,
                  error_msg=None, extra_msg=None):
        """
        write_msg takes some standard arguments, gets a json string from
        pipeline_json and writes it to a log

        Args:
        status -- a string like 'complete'
        job_start_secs -- the number of seconds from the epoch the job started
        error_msg -- a string of any error message
        extra_msg -- a string of any extra (non-error) information

        Returns:
        ---
        """
        sys.stderr.write("{0}\t{1}\n".format(
            self.stream,
            self._pipeline_json(status, job_start_secs, error_msg, extra_msg)
        ))
