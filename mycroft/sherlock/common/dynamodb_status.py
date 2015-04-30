# -*- coding: utf-8 -*-
"""
dynamodb_status.py is a dynamodb implementation of status table

At present these methods are intended to be a no-op.
We expect actual logging to be done externally

"""

from datetime import datetime, timedelta

from sherlock.common.status import StatusTableBase


class DynamoDbStatusTable(StatusTableBase):
    def __init__(self, logstatus, run_local=True):
        pass

    def log_status_result(self, conditions, work_time_secs,
                          database, failed=False, err_msg=None):
        """
        log_status_result updates a record in the status table based on
        what happened with a job.

        Args:
        conditions -- a dict with keys 'data_date' and 'table_versions'
        work_time_secs -- the integer amount of time the job took in seconds
        failed -- whether the job succeeded
        err_msg -- an error message to go in the status table

        Return:
        ---
        """

        # noop
        return

    def update_status(self, db, dd, versions, status,
                      start_time_secs=None, error_msg=None):
        """
        update_status updates the status of a job in the
        sdw_pipeline status table

        Args:
        db -- the Redshift database containing the status_table
        dd -- the date string of the data formatted YYYY/MM/DD
        versions -- the table versions of config.yaml
        status -- the new status
        start_time_secs -- the start time of the job in seconds from the epoch
        error_msg -- an optional error message

        Returns:
        ---
        """

        # noop
        return

    def query_et_complete_job(self, db, versions, date):
        """
        Query table for job that finished et step

        Args:
        db -- is the database we query
        version -- a string with <tablename>: <version> pairs
        date -- date for which mrjob has run

        """

        return [(date, None)]

    def query_et_complete_jobs(self, db, version, start_datetime):
        """
        Query table for jobs that finished et step starting at start_date

        Args:
        db -- is the database we query
        version -- a string with <tablename>: <version> pairs
        start_datetime -- earlist date to look for complete jobs

        """

        cur_datetime = start_datetime
        end_datetime = datetime.utcnow().date()
        results = []
        while cur_datetime <= end_datetime:
            data_date = datetime.strftime(cur_datetime, "%Y/%m/%d")
            results.append((data_date, None))
            cur_datetime += timedelta(days=1)
        return results

    def et_started(self, param_dict, database):
        """
        et_started checks if an et ('extract/transform') job already exists
        and returns a boolean

        Args:
        param_dict -- a dict with keys / values for the status record:
            'table_versions' -- a string with <tablename>: <version> pairs
            'data_date' -- the date to check

        Returns:
        True / False -- whether a record is there or not
        """

        return False

    def insert_et(self, input_dict, database):
        """
        insert_et inserts a record into the status table to show an et job
        has started

        Args:
        input_dict -- a dictionary with the following keys:
            data_date -- the date of the data for the et job
            table_versions -- a string of '<table_name>: <table_version>' pairs

        Returns:
        Boolean of whether the insert worked
        """

        # noop
        return
