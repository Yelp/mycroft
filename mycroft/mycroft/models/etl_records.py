# -*- coding: utf-8 -*-
'''
mycroft.models.etl_records persists metadata regarding each Extract-Transform-
Loading information.

ETLRecords is a collection of ETLRecord. ETLRecords implements iterable protocol.

ETLRecord contains metadata of an ETL run.

Example::

    >>> etl_records = ETLRecords(dynamo_table_object)
    >>> # Create
    >>> sucess = etl_records.put(data_date='2014-07-24',
            hash_key='1:public:search search_results')
    >>> sucess
    True
    >>> # Read
    >>> etl_record = etl_records.get(data_date='2014-07-24')
    >>> etl_record.get(data_date=None, job_status=None)
    {'data_date: '2014-07-24', 'job_status': 'et_started'}
    >>> # Update
    >>> success = etl_record.update('job_status'='et_cancel')
    >>> success
    True
    >>> etl_record.get(data_date=None, job_status=None)
    {'data_date': '2014-07-24', 'job_status': 'et_cancel'}
    >>> # Delete
    >>> etl_record.delete(user='awssoa', reason='Archiving')
    >>> # Get by job_id
    >>> records = etl_records.get_runs_with_job_id('abc123')
    >>> for record in records:
    >>>     print record.get(data_date=None, job_id=None)
    {'data_date': '2014-07-01', 'hash_key': 'abc123'}
    {'data_date': '2014-07-02', 'hash_key': 'abc123'}
    >>> # Iterate all records # it will be slow
    >>> for record in etl_records:
          print record
    <ETLRecord #some_id>
    <ETLRecord #some_other_id>

'''

from mycroft.models.abstract_records import Records


class ETLRecords(object):

    INDEX_JOB_ID_AND_DATA_DATE = 'ETLRecordByJobIdAndDataDate'

    def __init__(self, persistence_object=None, avro_schema_object=None):
        '''
        Private API. Unstable. Use it at your own risk.

        :param persistence_object: The implementation of a persistence_object;
            for example a dynamo table instance
        :param avro_schema_object: An Avro schema object that describes what's
            allowed in each ETLRecord
        :type avro_schema_object: Schema
        '''
        self._records = Records(
            persistence_object=persistence_object,
            avro_schema_object=avro_schema_object)

    def get(self, **kwargs):
        '''
        Returns an ETLRecord

        :param kwargs: all kwarg are used together to get the record. It's
            required to pass in at least one valid kwarg; an unknown kwarg, or
            no kwargs, will result in ValueError
        :returns: ETLRecord that matches given keys
        :rtype: :class:`.ETLRecord`
        :raises KeyError: if request record is not found
        :raises PrimaryKeyError: if request record does not have conforming
            primary key

        Example::
            >>> etl_record = etl_records.get(
                    hash_key='1:public:search search_results',
                    data_date='2014-07-26')
            >>> etl_record.get(data_date=None, s3_path=None)
            {'data_date': '2014-07-26',
                    's3_path': 's3://bucket/key1/schema.yaml'}

            >>> # Example of no kwarg
            >>> etl_records.get()
            ValueError
            >>> # Example of unknown kwarg
            >>> etl_records.get(color='black')
            ValueError

        '''
        return ETLRecord(self._records.get(**kwargs))

    def put(self, **kwargs):
        '''
        Puts an ETLRecord

        :param kwargs: each kwarg becomes key/value pair in the record.
            Passing in unknown kwarg will result in ValueError. If item
            already exists, ValueError will be raised.
        :returns: True if ETLRecords successfully persist the record
        :rtype: boolean
        :raises ValueError: if unknown kwarg is given
        :raises ValueError: if a duplicate record already exists
        :raises PrimaryKeyError: if the given record does not have a
            conforming primary key

        Example::
            >>> success = etl_records.put(
                    hash_key='1:public:search search_results',
                    data_date='2014-07-26', et_state='et_started')
            >>> success
            True
            >>> # Trying to put the same item again
            >>> success = etl_records.put(
                    hash_key='1:public:search search_results',
                    data_date='2014-07-26', et_state='et_started')
            >>> ValueError

            >>> # Example of no kwarg
            >>> etl_records.put()
            ValueError
            >>> # Example of unknown kwarg
            >>> etl_records.put(color='black')
            ValueError
        '''
        return self._records.put(**kwargs)

    def get_runs_with_job_id(self, job_id, data_date=None):
        '''
        Get ETLRecord matching given job_id

        :param job_id: id of the job
        :type job_id: string
        :returns: An iterable of ETLRecord matching given job_id

        Example::
            >>> jobs = scheduled_jobs.get_jobs_with_job_id('1af2')
            >>> for job in job:
                  print job.get(hash_key=None, data_date=None)
            {'hash_key': '1af2', 'data_date': '2014-07-01'}
            {'hash_key': '1af2', 'data_date': '2014-07-02'}
        '''
        records = self._records.query_by_index(
            index=self.INDEX_JOB_ID_AND_DATA_DATE,
            job_id=job_id, data_date=data_date)

        def iter_record():
            for record in records:
                yield ETLRecord(record=record)

        return iter_record()

    def delete_job_runs(self, job_id):
        '''
        Attempt to delete all runs for a job id

        :param job_id: id of the job
        :type job_id: string
        :returns: True if all runs are successfully deleted
        :type: boolean
        '''
        runs = self.get_runs_with_job_id(job_id)
        self._records.batch_delete(runs, hash_key=job_id, data_date=None)
        runs = self.get_runs_with_job_id(job_id)
        return len([r for r in runs]) == 0

    def __iter__(self):

        def iter_records():
            for record in self._records:
                yield ETLRecord(record=record)

        return iter_records()


class ETLRecord(object):

    def __init__(self, record=None):
        '''
        Unstable API. Use at your own risk.

        :param record: the underlying storage that represents the record
        :type record: mycroft.models.abstract_records.Record
        '''
        self._record = record

    def update(self, **kwargs):
        '''
        Updates the record

        :returns: True if ETLRecord successfully update
        :rtype: boolean
        :raises ValueError: if there is no kwargs

        Example::
            >>> etl_record.get(data_date=None, et_state='et_started')
            {'data_date': '2014-07-26', 'et_state': 'et_started'}
            >>> success = etl_record.update(et_state='et_completed')
            >>> success
            True
            >>> etl_record.get(data_date=None, et_state=None)
            {'data_date': '2014-07-26', 'et_state': 'et_completed'}

        '''
        return self._record.update(**kwargs)

    def delete(self, user, reason):
        '''
        Deletes the record

        :param user: username who requests the deletion of the item
        :type user: string
        :param reason: a brief description on why the item is being indexed.
        :type reason: string
        :returns: True if ETLRecord is successfully deleted
        :type: boolean
        :raises ValueError: if user is None or a string with just whitespaces
        :raises ValueError: if reason is None or a string with just whitespaces

        Example::

            >>> success = etl_record.delete('awssoa', 'archiving')
            >>> success
            True

        '''
        return self._record.delete(user, reason)

    def get(self, **kwargs):
        '''
        Returns a dictionary of key-value pair specified in the kwargs

        :param kwargs: each kwarg will be fetch from the record
        :returns: a dictionary
        :rtype: a dictionary
        :raises ValueError: if unknown kwarg is given

        Example::

            >>> etl_record.get(data_date=None, run_id=None)
            {'data_date': '2014-07-24', 'run_id': '9413984uy31984u31894'}

            >>> # Example of no kwarg
            >>> etl_record.get()
            ValueError
            >>> # Example of unknown kwarg
            >>> etl_record.get(color='black')
            ValueError


        '''
        return self._record.get(**kwargs)
