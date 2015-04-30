# -*- coding: utf-8 -*-
from copy import copy

from boto.dynamodb2.fields import GlobalAllIndex
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.fields import RangeKey
from boto.dynamodb2.table import Table
import pytest

from mycroft.models.aws_connections import get_avro_schema
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_RUNNING
from mycroft.models.scheduled_jobs import ScheduledJobs
from tests.data.scheduled_job import SAMPLE_LOG_NAME_RANGER
from tests.data.scheduled_job import SAMPLE_LOG_SCHEMA_VERSION_1
from tests.data.scheduled_job import SAMPLE_RECORD_ET_STATUS_RUNNING_1
from tests.data.scheduled_job import SAMPLE_RECORD_ET_STATUS_RUNNING_2
from tests.data.scheduled_job import SAMPLE_RECORD_ET_STATUS_COMPLETE_3
from tests.models.test_abstract_records import dynamodb_connection  # noqa
from tests.models.test_abstract_records import NAME_TO_SCHEMA


class FakeScheduledJob(object):
    def __init__(self, fake_input_record):
        """
        FakeScheduledJob supports a partial interface for ScheduledJob
        namely, the __init__ and get methods
        """
        self._fake_record = copy(fake_input_record)

    def get(self, **kwargs):
        output_dict = dict((key, self._fake_record.get(key, value))
                           for key, value in kwargs.iteritems())
        return output_dict

    def update(self, **kwargs):
        self._fake_record.update(**kwargs)
        # dictionary's update does not return any value!
        return True


@pytest.yield_fixture  # noqa
def scheduled_jobs(dynamodb_connection):
    tsj = TestScheduledJobs()
    table, scheduled_jobs = tsj._get_scheduled_jobs(dynamodb_connection)
    assert scheduled_jobs.put(**SAMPLE_RECORD_ET_STATUS_RUNNING_1)
    assert scheduled_jobs.put(**SAMPLE_RECORD_ET_STATUS_RUNNING_2)
    assert scheduled_jobs.put(**SAMPLE_RECORD_ET_STATUS_COMPLETE_3)
    yield scheduled_jobs
    assert table.delete()


class TestScheduledJobs(object):

    @classmethod
    def create_fake_scheduled_job(cls, keyword_dict):
        return FakeScheduledJob(keyword_dict)

    def _get_scheduled_jobs(self, dynamodb_connection):  # noqa
        """
        WARNING -- this method requires cleanup; the user must remember to
        delete the table once complete.  For example:

        >>> NEW_JOB = {'log_version': 'ad_click', 'log_schema_version': '1'}
        >>> def cool_test_fn(dynamodb_connection):
        >>>     tsj = TestScheduledJobs()
        >>>     table, scheduled_jobs = tsj._get_scheduled_jobs(dynamodb_connection)
        >>>     assert scheduled_jobs.put(**NEW_JOB)
        >>>     yield scheduled_jobs
        >>>     assert table.delete()  # THIS IS THE KEY CLEANUP!!

        """
        avro_schema = get_avro_schema('mycroft/avro/scheduled_jobs.json')
        index_load_status = GlobalAllIndex(
            ScheduledJobs.INDEX_LOAD_STATUS,
            parts=[HashKey('load_status')])
        index_et_status = GlobalAllIndex(
            ScheduledJobs.INDEX_ET_STATUS,
            parts=[HashKey('et_status')])
        index_load_status = GlobalAllIndex(
            ScheduledJobs.INDEX_LOAD_STATUS,
            parts=[HashKey('load_status')])
        index_log_name_and_log_schema_version = GlobalAllIndex(
            ScheduledJobs.INDEX_LOG_NAME_AND_LOG_SCHEMA_VERSION,
            parts=[HashKey('log_name'), RangeKey('log_schema_version')])
        table = Table.create(
            'ScheduledJobs',
            schema=NAME_TO_SCHEMA['scheduled_jobs'],
            connection=dynamodb_connection,
            global_indexes=[index_et_status, index_load_status,
                            index_log_name_and_log_schema_version])
        return table, ScheduledJobs(persistence_object=table, avro_schema_object=avro_schema)

    def test_get_jobs_with_et_status(self, scheduled_jobs):
        # query by a value
        # make sure it includes only the value we are looking for
        jobs = scheduled_jobs.get_jobs_with_et_status(JOBS_ETL_STATUS_RUNNING)
        # We have to convert the iterable to list because
        # there are multiple properties we want to verify
        jobs = [job for job in jobs]
        assert all([
            job.get(et_status=None)['et_status'] == JOBS_ETL_STATUS_RUNNING
            for job in jobs
        ])
        assert len(jobs) == 2

    def test_get_jobs_with_log_name(self, scheduled_jobs):
        # query by a value
        # make sure it includes only the value we are looking for
        jobs = scheduled_jobs.get_jobs_with_log_name(SAMPLE_LOG_NAME_RANGER)
        # We have to convert the iterable to list because
        # there are multiple properties we want to verify
        jobs = [job for job in jobs]
        assert all([job.get(log_name=None)['log_name'] == SAMPLE_LOG_NAME_RANGER for job in jobs])
        assert len(jobs) == 2

    def test_get_jobs_with_log_name_and_log_schema_version(self, scheduled_jobs):
        # query by a value
        # make sure it includes only the value we are looking for
        jobs = scheduled_jobs.get_jobs_with_log_name(
            SAMPLE_LOG_NAME_RANGER,
            log_schema_version=SAMPLE_LOG_SCHEMA_VERSION_1)
        # We have to convert the iterable to list because
        # there are multiple properties we want to verify
        jobs = [job for job in jobs]
        assert len(jobs) == 1
        assert jobs[0].get(log_name=None, log_schema_version=None) == {
            'log_name': SAMPLE_LOG_NAME_RANGER,
            'log_schema_version': SAMPLE_LOG_SCHEMA_VERSION_1}
