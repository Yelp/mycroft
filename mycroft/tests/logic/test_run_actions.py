# -*- coding: utf-8 -*-
import pytest

from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.fields import RangeKey
from boto.dynamodb2.fields import GlobalAllIndex
from boto.dynamodb2.table import Table

from mycroft.models.aws_connections import get_avro_schema
from mycroft.models.etl_records import ETLRecords

from mycroft.logic.run_actions import _parse_runs
from mycroft.logic.run_actions import list_runs_by_job_id

from tests.models.test_abstract_records import dynamodb_connection  # noqa
from tests.models.test_abstract_records import NAME_TO_SCHEMA
from tests.models.test_etl_record import FakeETLRecord
from tests.data.etl_record import SAMPLE_JOB_ID
from tests.data.etl_record import SAMPLE_RECORD_JOBS


BASE_DICT = {
    'hash_key': None,
    'data_date': None,
    'etl_status': None,
    'et_runtime': None,
    'et_starttime': None,
    'load_runtime': None,
    'load_starttime': None,
    'redshift_id': None,
    's3_path': None,
    'updated_at': None,
    'run_by': None,
    'job_id': None,
    'etl_error': None,
    'additional_arguments': None,
}


class TestRunActions(object):

    @pytest.yield_fixture(scope='module')  # noqa
    def etl_records(self, dynamodb_connection):
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
        for job in SAMPLE_RECORD_JOBS:
            assert etl_records.put(**job)
        yield etl_records
        assert table.delete()

    def test__parse_runs_empty_run(self):
        empty_runs = [FakeETLRecord(BASE_DICT)]
        result = _parse_runs(empty_runs)
        assert result['runs'][0] == BASE_DICT

    def test_list_runs_by_job_id(self, etl_records):
        return_value = list_runs_by_job_id(SAMPLE_JOB_ID, etl_records)
        expected_count = len([job for job in SAMPLE_RECORD_JOBS
                              if job['job_id'] == SAMPLE_JOB_ID])
        assert len(return_value['runs']) == expected_count

    @pytest.mark.parametrize("job_id", ['y', '..', '!', '', '_'])
    def test_list_runs_by_job_id_bad_job_id(self, job_id):
        with pytest.raises(ValueError) as e:
            list_runs_by_job_id(job_id, None)
        assert e.exconly().startswith("ValueError: invalid job_id")
