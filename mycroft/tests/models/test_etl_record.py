# -*- coding: utf-8 -*-
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.fields import RangeKey
from boto.dynamodb2.fields import GlobalAllIndex
from boto.dynamodb2.table import Table
import pytest

from mycroft.models.etl_records import ETLRecords
from tests.data.etl_record import SAMPLE_JOB_ID
from tests.data.etl_record import SAMPLE_RECORD_JOBS
from tests.models.test_abstract_records import dynamodb_connection  # noqa
from mycroft.models.aws_connections import get_avro_schema
from tests.models.test_abstract_records import NAME_TO_SCHEMA


class FakeETLRecord(object):
    def __init__(self, fake_input_record):
        """
        FakeETLRecord supports a partial interface for ETLRecord
        namely, the __init__ and get methods
        """
        self._fake_record = fake_input_record

    def get(self, **kwargs):
        output_dict = dict((key, self._fake_record.get(key, value))
                           for key, value in kwargs.iteritems())
        return output_dict


@pytest.yield_fixture  # noqa
def etl_records(dynamodb_connection):
    avro_schema = get_avro_schema('mycroft/avro/etl_record.json')
    index_job_id = GlobalAllIndex(
        ETLRecords.INDEX_JOB_ID_AND_DATA_DATE,
        parts=[HashKey('job_id'), RangeKey('data_date')])
    table = Table.create(
        'ETLRecords',
        schema=NAME_TO_SCHEMA['etl_records'],
        connection=dynamodb_connection,
        global_indexes=[index_job_id])
    etl_records = ETLRecords(persistence_object=table, avro_schema_object=avro_schema)
    for etl_record in SAMPLE_RECORD_JOBS:
        assert etl_records.put(**etl_record)
    yield etl_records
    assert table.delete()


class TestETLRecords(object):

    def test_get_jobs_with_job_id(self, etl_records):
        # query by a value
        # make sure it includes only the value we are looking for
        runs = etl_records.get_runs_with_job_id(SAMPLE_JOB_ID)
        # We have to convert the iterable to list because
        # there are multiple properties we want to verify
        etl_runs = [run for run in runs]
        assert all([run.get(job_id=None)['job_id'] == SAMPLE_JOB_ID for run in etl_runs])
        assert len(etl_runs) == 2
