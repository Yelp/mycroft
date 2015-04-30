# -*- coding: utf-8 -*-
'''
mycroft.models.scheduled_jobs persists metadata regarding each scheduled job

ScheduledJobs is a collection of ScheduledJob. ScheduledJobs implements iterable protocol.

ScheduledJob contains metadata of a scheduled job.

Example::

    >>> scheduled_jobs = ScheduledJobs(dynamo_table_object)
    >>> # Create
    >>> sucess = scheduled_jobs.put(s3_path='s3://my-bucket/logs/apache',
            hash_key='1:public:my_cool_schema_alpha:2014-08-01:2014-08-02')
    >>> sucess
    True
    >>> # Read
    >>> scheduled_job = scheduled_jobs.get(
            hash_key='1:public:my_cool_schema_alpha:2014-08-01:2014-08-02')
    >>> scheduled_job.get(s3_path=None)
    {'s3_path': 's3://my-bucket/logs/apache'}
    >>> # Update
    >>> success = scheduled_job.update(s3_path='s3://my-bucket/logs/ranger')
    >>> success
    True
    >>> scheduled_job.get(s3_path=None)
    {'s3_path': 's3://my-bucket/logs/ranger'}
    >>> # Delete
    >>> scheduled_job.delete(user='awssoa', reason='Archiving')
    >>> # Iterate all jobs # it will be slow...
    >>> for job in scheduled_jobs:
          print job
    <ScheduledJob #some_id>
    <ScheduledJob #some_other_id>

'''
from mycroft.models.abstract_records import Records
import datetime
import os


JOBS_ETL_STATUS_SUCCESS = 'success'
JOBS_ETL_STATUS_RUNNING = 'running'
JOBS_ETL_STATUS_CANCELLED = 'cancelled'
JOBS_ETL_STATUS_PAUSED = 'paused'
JOBS_ETL_STATUS_SCHEDULED = 'scheduled'
JOBS_ETL_STATUS_COMPLETE = 'complete'
JOBS_ETL_STATUS_ERROR = 'error'
JOBS_ETL_STATUS_EMPTY = 'null'
JOBS_ETL_STATUS_DELETED = 'deleted'

JOBS_ETL_STATUSES_FINAL = [
    JOBS_ETL_STATUS_CANCELLED,
    JOBS_ETL_STATUS_COMPLETE,
]

JOBS_ETL_STATUSES_SCHEDULABLE = [
    JOBS_ETL_STATUS_EMPTY,
    JOBS_ETL_STATUS_SUCCESS,
]

JOBS_ETL_STATUS_TRANSITION_TABLE = {
    JOBS_ETL_STATUS_EMPTY: [
        JOBS_ETL_STATUS_SCHEDULED,
    ],
    JOBS_ETL_STATUS_SCHEDULED: [
        JOBS_ETL_STATUS_EMPTY,      # for re-submission
        JOBS_ETL_STATUS_RUNNING,
    ],
    JOBS_ETL_STATUS_RUNNING: [
        JOBS_ETL_STATUS_CANCELLED,
        JOBS_ETL_STATUS_EMPTY,      # for re-submission
        JOBS_ETL_STATUS_RUNNING,    # for keepalive
        JOBS_ETL_STATUS_SUCCESS,
        JOBS_ETL_STATUS_ERROR,
        JOBS_ETL_STATUS_COMPLETE,
        JOBS_ETL_STATUS_PAUSED,
    ],
    JOBS_ETL_STATUS_SUCCESS: [
        JOBS_ETL_STATUS_SCHEDULED,
    ],
    JOBS_ETL_STATUS_CANCELLED: [
        JOBS_ETL_STATUS_EMPTY,      # for deletion
    ],
    JOBS_ETL_STATUS_COMPLETE: [
        JOBS_ETL_STATUS_EMPTY,      # for deletion
    ],
    JOBS_ETL_STATUS_ERROR: [
        JOBS_ETL_STATUS_EMPTY,      # for retry
    ],
    JOBS_ETL_STATUS_PAUSED: [
        JOBS_ETL_STATUS_EMPTY,      # for resume
        JOBS_ETL_STATUS_SUCCESS,
    ],
}


JOBS_ETL_ACTIONS = ['cancel_requested', 'pause_requested', 'delete_requested']


class ScheduledJobs(object):

    INDEX_ET_STATUS = 'ETStatusIndex'
    INDEX_LOAD_STATUS = 'LoadStatusIndex'
    INDEX_LOG_NAME_AND_LOG_SCHEMA_VERSION = 'LogNameLogSchemaVersionIndex'

    def __init__(self, persistence_object=None, avro_schema_object=None):
        '''
        Private API. Unstable. Use it at your own risk.

        :param persistence_object: The implementation of a persistence_object;
            for example a dynamo table instance
        :param avro_schema_object: An Avro schema object that describes what's
            allowed in each ScheduledJob
        :type avro_schema_object: Schema
        '''
        self._records = Records(
            persistence_object=persistence_object,
            avro_schema_object=avro_schema_object)
        self.username = os.getenv('LOGNAME')

    def get(self, **kwargs):
        '''
        Returns an ScheduledJob

        :param kwargs: all kwarg are used together to get the job. It's
            required to pass in at least one valid kwarg; an unknown kwarg, or
            no kwargs, will result in ValueError
        :returns: ScheduledJob that matches given keys
        :rtype: :class:`.ScheduledJob`
        :raises KeyError: if request record is not found
        :raises PrimaryKeyError: if request record does not have conforming
            primary key

        Example::
            >>> scheduled_job = scheduled_jobs.get(
                    hash_key='1:public:my_cool_schema_alpha:2014-08-01:2014-08-02')
            >>> scheduled_job.get(s3_path=None)
            {'s3_path': 's3://my-bucket/logs/apache'}
            >>> # Example of no kwarg
            >>> scheduled_jobs.get()
            ValueError
            >>> # Example of unknown kwarg
            >>> scheduled_jobs.get(color='black')
            ValueError

        '''
        return ScheduledJob(self._records.get(**kwargs))

    def put(self, **kwargs):
        '''
        Puts an ScheduledJob

        :param kwargs: each kwarg becomes key/value pair in the job.
            Passing in unknown kwarg will result in ValueError. If item
            already exists, ValueError will be raised.
        :returns: True if ScheduledJobs successfully persist the job
        :rtype: boolean
        :raises ValueError: if unknown kwarg is given
        :raises ValueError: if a duplicate record already exists
        :raises PrimaryKeyError: if the given record does not have a
            conforming primary key

        Example::
            >>> sucess = scheduled_jobs.put(s3_path='s3://my-bucket/logs/apache',
                    hash_key='1:public:my_cool_schema_alpha:2014-08-01:2014-08-02')
            >>> sucess
            True
            >>> # Trying to put the same item again
            >>> sucess = scheduled_jobs.put(s3_path='s3://my-bucket/logs/apache',
                    hash_key='1:public:my_cool_schema_alpha:2014-08-01:2014-08-02')
            >>> ValueError

            >>> # Example of no kwarg
            >>> scheduled_jobs.put()
            ValueError
            >>> # Example of unknown kwarg
            >>> scheduled_jobs.put(color='black')
            ValueError
        '''
        return self._records.put(**kwargs)

    def delete(self, **kwargs):
        '''
        Delete

        :param kwargs: all kwarg are used together to get the job before deleting it.
            It's required to pass in at least one valid kwarg; an unknown kwarg, or
            no kwargs, will result in ValueError
        :returns: True if ScheduledJob is successfully deleted
        :type: boolean
        :raises KeyError: if request record is not found
        :raises PrimaryKeyError: if request record does not have conforming
            primary key

        Example::
            >>> scheduled_job = scheduled_jobs.delete(
                    hash_key='1:public:my_cool_schema_alpha:2014-08-01:2014-08-02')
        '''
        job = self.get(**kwargs)
        return job.delete(self.username, 'user request')

    def get_jobs_with_et_status(self, et_status_value):
        '''
        Get ScheduledJob matching given et_status

        :param et_status_value: value of et_status
        :type et_status_value: string
        :returns: An iterable of ScheduledJob matching given et_status

        Example::
            >>> jobs = scheduled_jobs.get_jobs_with_et_status('running')
            >>> for job in job:
                  print job.get(hash_key=None, et_status=None)
            {'hash_key': '1', 'et_status': 'running'}
            {'hash_key': '2', 'et_status': 'running'}
        '''
        records = self._records.query_by_index(
            index=self.INDEX_ET_STATUS,
            et_status=et_status_value)

        def iter_record():
            for record in records:
                yield ScheduledJob(record=record)

        return iter_record()

    def get_jobs_with_log_name(self, log_name, log_schema_version=None):
        '''
        Get ScheduledJob matching given log_name and optional log_schema_version

        :param log_name: value of log_name
        :type log_name: string
        :param log_schema_version: optional value of log_schema_version
        :type log_schema_version: string or None
        :returns: An iterable of ScheduledJob matching given values

        Example::
            >>> scheduled_jobs = ScheduleJobs()
            >>> jobs = scheduled_jobs.get_jobs_with_log_name('ranger')
            >>> for job in job:
                  print job.get(hash_key=None, log_name=None)
            {'hash_key': '1', 'log_name': 'ranger'}
            {'hash_key': '2', 'log_name': 'ranger'}
        '''
        records = self._records.query_by_index(
            index=self.INDEX_LOG_NAME_AND_LOG_SCHEMA_VERSION,
            log_name=log_name,
            log_schema_version=log_schema_version)

        def iter_record():
            for record in records:
                yield ScheduledJob(record=record)

        return iter_record()

    def __iter__(self):

        def iter_records():
            for record in self._records:
                yield ScheduledJob(record=record)

        return iter_records()


class ScheduledJobInvalidStatusUpdate(Exception):
    pass


class ScheduledJob(object):

    def __init__(self, record=None):
        '''
        Unstable API. Use at your own risk.

        :param record: the underlying storage that represents the job
        :type record: mycroft.models.abstract_records.Record
        '''
        self._record = record

    def _validate_status_update(self, status_field, new_status):

        if status_field not in ['et_status']:
            raise ValueError("Invalid status field: {0}".format(status_field))

        kwargs = {status_field: None}
        cur_kwargs = self.get(**kwargs)
        cur_status = cur_kwargs[status_field]

        if new_status not in JOBS_ETL_STATUS_TRANSITION_TABLE[cur_status]:
            raise ScheduledJobInvalidStatusUpdate(
                "cur_status={0}, new_status={1}".format(cur_status, new_status)
            )

    def update(self, **kwargs):
        '''
        Updates the job

        :returns: True if ScheduledJob successfully update
        :rtype: boolean
        :raises ValueError: if there is no kwargs

        Example::
            >>> scheduled_job.get(s3_path=None)
            {'s3_path': 's3://my-bucket/logs/apache'}
            >>> # Update
            >>> success = scheduled_job.update(s3_path='s3://my-bucket/logs/ranger')
            >>> success
            True
            >>> scheduled_job.get(s3_path=None)
            {'s3_path': 's3://my-bucket/logs/ranger'}
        '''

        # take special care for status field
        field = 'et_status'
        timestamp_field = field + '_last_updated_at'
        if field in kwargs:
            self._validate_status_update(field, kwargs[field])
            if timestamp_field not in kwargs:
                kwargs[timestamp_field] = str(datetime.datetime.utcnow())

        return self._record.update(**kwargs)

    def delete(self, user, reason):
        '''
        Deletes the job

        :param user: username who requests the deletion of the job
        :type user: string
        :param reason: a brief description on why the job is being deleted.
        :type reason: string
        :returns: True if ScheduledJob is successfully deleted
        :type: boolean
        :raises ValueError: if user is None or a string with just whitespaces
        :raises ValueError: if reason is None or a string with just whitespaces

        Example::

            >>> success = scheduled_job.delete('awssoa', 'archiving')
            >>> success
            True

        '''
        return self._record.delete(user, reason)

    def get(self, **kwargs):
        '''
        Returns a dictionary of key-value pair specified in the kwargs

        :param kwargs: each kwarg will be fetch from the job
        :returns: a dictionary
        :rtype: a dictionary
        :raises ValueError: if unknown kwarg is given

        Example::

            >>> scheduled_job.get(s3_path=None)
            {'s3_path': 's3://my-bucket/logs/apache'}

            >>> # Example of no kwarg
            >>> scheduled_job.get()
            ValueError
            >>> # Example of unknown kwarg
            >>> scheduled_job.get(color='black')
            ValueError


        '''
        return self._record.get(**kwargs)
