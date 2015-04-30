# -*- coding: utf-8 -*-
import pytest
from tests.models.test_etl_record import etl_records  # noqa
from tests.models.test_abstract_records import dynamodb_connection  # noqa
from mycroft.backend.worker.etl_status_helper import ETLStatusHelper
import mock


RECORDS = [
    {'status': 'error', 'date': '2014-09-01', 'start_time': 4, 'end_time': 10,
        'error_info': {'crash_a': 'error_a', 'crash_b': 'error_b'}},
    {'status': 'success', 'date': '2014-09-02', 'start_time': 6, 'end_time': 12,
        'error_info': {}},
]

MSG = {
    'uuid': 'some-uuid',
    'redshift_id': 'some-rs-id',
}


KWARGS = {
    'hash_key': None,
    'etl_status': None,
    'et_starttime': None,
    'load_starttime': None,
    'data_date': None,
    'run_by': None,
    'redshift_id': None,
    'job_id': None,
    'etl_error': None,
}


class TestETLStatusHelper(object):

    @pytest.yield_fixture  # noqa
    def get_etl_helper(self, etl_records):
        with mock.patch(
                'mycroft.models.aws_connections.TableConnection.get_connection'
                ) as mocked_etl:
            mocked_etl.return_value = etl_records
            yield ETLStatusHelper()

    def test_etl_step_started(self, get_etl_helper):
        etl = get_etl_helper
        for r in RECORDS:
            date = r['date']
            step = 'et'

            # run twice to hit new and update record cases
            etl.etl_step_started(MSG, date, step)
            etl.etl_step_started(MSG, date, step)

            entry = etl.etl_db.get(hash_key='some-uuid', data_date=date)
            entry_dict = entry.get(**KWARGS)
            assert entry_dict['hash_key'] == 'some-uuid'
            assert entry_dict['data_date'] == date

        with pytest.raises(ValueError):
            etl.etl_step_started(MSG, None, 'et')

    def test_etl_step_complete(self, get_etl_helper):
        etl = get_etl_helper
        for r in RECORDS:
            date = r['date']
            step = 'et'

            # test case: no previous record
            etl.etl_step_complete(MSG, date, step, r)

            # test case: existing record
            etl.etl_step_started(MSG, date, step)
            etl.etl_step_complete(MSG, date, step, r)

            entry = etl.etl_db.get(hash_key='some-uuid', data_date=date)
            entry_dict = entry.get(**KWARGS)
            assert entry_dict['hash_key'] == 'some-uuid'
            assert entry_dict['data_date'] == date

            if entry_dict['etl_status'] == 'load_success':
                assert entry_dict.get('etl_error') is None
            elif entry_dict['etl_status'] == 'load_error':
                assert entry_dict['etl_error'] == str(r['error_info'])

        with pytest.raises(ValueError):
            etl.etl_step_complete(MSG, None, 'et', RECORDS[0])
