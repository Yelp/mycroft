# -*- coding: utf-8 -*-
"""
status.py is a collection of functions used to manipulate status table

"""

from datetime import datetime


class StatusRecord(object):
    """
    StatusRecord is a class for handling records of status in sql-like table
    """

    def __init__(self, data_date=None, updated_at=None, et_status=None,
                 et_runtime=None, et_finished=None, load_status=None,
                 load_runtime=None, load_finished=None, table_versions=None,
                 et_version=None, load_version=None, error_message=None):

        self.record = {
            'data_date': data_date,
            'updated_at': updated_at,
            'et_status': et_status,
            'et_runtime': et_runtime,
            'et_finished': et_finished,
            'load_status': load_status,
            'load_runtime': load_runtime,
            'load_finished': load_finished,
            'et_version': et_version,
            'load_version': load_version,
            'table_versions': table_versions,
            'error_message': error_message}

    def set_updated_at(self):
        """
        sets the updated_at field to the current utc time
        """
        self.record['updated_at'] = datetime.utcnow()

    def set_data_date(self, data_date):
        """
        sets the data_date field to the date specified
        """
        self.record['data_date'] = data_date

    def minimized_record(self):
        """
        minimized record creates a dictionary from the current record
        eliminating any k, v pairs where the value is None.
        """
        return dict(
            (k, v) for k, v in self.record.iteritems() if v is not None
        )

    def get_set_clause(self):
        """
        returns a string using the objects state of the form:
            " key = %(key)s,"
        usable as a partial input to psycopg2.  We sort the keys prior to
        building the string to aid in testing
        """
        command_list = ["{0} = %({1})s".format(k, k)
                        for k in sorted(self.minimized_record())]
        return ", ".join(command_list)


class StatusTableBase(object):
    def __init__(self):
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
        raise NotImplementedError

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
        raise NotImplementedError

    def query_et_complete_job(self, db, versions, date):
        """
        Query table for job that finished et step

        Args:
        db -- is the database we query
        version -- a string with <tablename>: <version> pairs
        date -- date for which mrjob has run

        """
        raise NotImplementedError

    def query_et_complete_jobs(self, db, version, start_datetime):
        """
        Query table for jobs that finished et step starting at start_date

        Args:
        db -- is the database we query
        version -- a string with <tablename>: <version> pairs
        start_datetime -- earlist date to look for complete jobs

        """
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError
