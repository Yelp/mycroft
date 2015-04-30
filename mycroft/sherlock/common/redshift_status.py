# -*- coding: utf-8 -*-
"""
redshift_status.py is a Redshfit implementation of status table

"""

from sherlock.common.status import StatusRecord, StatusTableBase
from datetime import datetime
import time

# sdw_pipline_status column sizes
VERSIONS_COL_SIZE = 256
ERROR_MSG_COL_SIZE = VERSIONS_COL_SIZE

UPDATE_JOB = """UPDATE sdw_pipeline_status SET {0} \
WHERE data_date = %(data_date)s AND table_versions = %(table_versions)s"""

INSERT = "INSERT INTO sdw_pipeline_status ({0}) VALUES ({1})"

READ_JOB_STATUS = """SELECT et_status, load_status \
FROM sdw_pipeline_status \
WHERE data_date=%(data_date)s AND table_versions=%(table_versions)s"""

QUERY_COMPLETE_JOB = """SELECT data_date, load_status \
FROM sdw_pipeline_status \
WHERE et_status='complete' \
AND table_versions=%(table_versions)s \
AND data_date=%(data_date)s"""

QUERY_COMPLETE_JOBS = """SELECT data_date, load_status \
FROM sdw_pipeline_status \
WHERE data_date >= %(start_ts)s \
AND et_status = 'complete' \
AND table_versions = %(table_versions)s \
ORDER BY data_date ASC"""


def get_update_string(clause):
        """
        returns a string using the state of this status record, usable for
        an update in psycopg2.
        """
        return UPDATE_JOB.format(clause)


def get_insert_string(record):
        """
        returns a string using the state of this status record, usuable for
        an insert in psycopg2.  We sort the keys prior to buildling the string
        to aid in testing.
        """
        key_string = ", ".join(["{0}".format(k)
                               for k in sorted(record)])
        value_string = ", ".join(["%({0})s".format(k)
                                 for k in sorted(record)])
        return INSERT.format(key_string, value_string)


class RedshiftStatusTable(StatusTableBase):
    """ Redshift implementation of StatusTableBase
    """

    def __init__(self, psql):
        super(RedshiftStatusTable, self).__init__()
        self.psql = psql

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

        status = "error" if failed else "complete"
        if err_msg:
            err_msg = "{0}".format(err_msg)[:ERROR_MSG_COL_SIZE]
        finish_time = datetime.utcnow()
        update = StatusRecord(
            et_status=status,
            et_runtime=work_time_secs,
            et_finished=finish_time,
            error_message=err_msg, updated_at=finish_time
        )
        update_string = get_update_string(update.get_set_clause())
        update.record['data_date'] = conditions['data_date']
        update.record['table_versions'] = conditions['table_versions']
        params_dict = update.minimized_record()
        self.psql.run_sql(
            update_string, database, 'update', params=params_dict
        )

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
        utc_now = datetime.utcnow()
        params = {'load_status': status, 'updated_at': utc_now}
        if start_time_secs is not None:
            params['load_runtime'] = int(time.time() - start_time_secs)
            params['load_finished'] = utc_now
        if error_msg:
            params['error_message'] = error_msg[:ERROR_MSG_COL_SIZE]
        sr = StatusRecord(**params)
        params['table_versions'] = versions
        params['data_date'] = dd
        self.psql.run_sql(get_update_string(sr.get_set_clause()), db,
                          'updating status', params=params)

    def query_et_complete_job(self, db, versions, date):
        """
        Query table for job that finished et step

        Args:
        db -- is the database we query
        version -- a string with <tablename>: <version> pairs
        date -- date for which mrjob has run

        """
        params = {'table_versions': versions}
        params['data_date'] = date

        return self.psql.run_sql(
            QUERY_COMPLETE_JOB, db, 'select status', params=params, output=True
        )

    def query_et_complete_jobs(self, db, version, start_datetime):
        """
        Query table for jobs that finished et step starting at start_date

        Args:
        db -- is the database we query
        version -- a string with <tablename>: <version> pairs
        start_datetime -- earlist date to look for complete jobs

        """
        params = {'table_versions': version}
        params['start_ts'] = start_datetime.strftime("%Y/%m/%d")

        return self.psql.run_sql(
            QUERY_COMPLETE_JOBS, db, 'select status',
            params=params, output=True
        )

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
        if len(param_dict['table_versions']) > VERSIONS_COL_SIZE:
            raise ValueError(
                "table versions are too big to fit into pipeline status"
            )

        query_results = self.psql.run_sql(
            READ_JOB_STATUS,
            database,
            'check job status for data',
            params=param_dict, output=True
        )
        return bool(query_results)

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
        insert_record = StatusRecord(
            data_date=input_dict['data_date'],
            et_status="started",
            table_versions=input_dict['table_versions']
        )
        insert_record.set_updated_at()
        return self.psql.run_sql(
            get_insert_string(insert_record.minimized_record()),
            database,
            'insert',
            params=insert_record.minimized_record()
        )
