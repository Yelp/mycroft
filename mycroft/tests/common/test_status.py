# -*- coding: utf-8 -*-
from datetime import datetime
from datetime import timedelta
import mock
import pytest

from sherlock.common.redshift_status import RedshiftStatusTable
from sherlock.common.redshift_status import VERSIONS_COL_SIZE
from sherlock.common.dynamodb_status import DynamoDbStatusTable


@pytest.fixture(scope="module")
def psql():
    with mock.patch('sherlock.common.redshift_psql.RedshiftPostgres',
                    autospec=True) as mock_psql:
        instance = mock_psql.return_value
        instance.run_sql.return_value = None
        return mock_psql


@pytest.mark.parametrize("method_name, input_args, expected_args", [
    ('log_status_result',
     [{'data_date': 'date', 'table_versions': 'v1'}, 5, 'db', False, None],
     {'et_status': 'complete', 'data_date': 'date', 'table_versions': 'v1'}),
    ('log_status_result',
     [{'data_date': 'date', 'table_versions': 'v1'}, 5, 'db', True, 'failed'],
     {'et_status': 'error', 'data_date': 'date', 'table_versions': 'v1',
      'error_message': 'failed'}),
    ('update_status',
     ['db', 'date', 'v1', 'complete', 0, None],
     {'load_status': 'complete', 'data_date': 'date', 'table_versions': 'v1'}),
    ('update_status',
     ['db', 'date', 'v1', 'error', 0, 'failed'],
     {'load_status': 'error', 'data_date': 'date', 'table_versions': 'v1',
      'error_message': 'failed'}),
    ('query_et_complete_job',
     ['db', 'v1', 'date'],
     {'data_date': 'date', 'table_versions': 'v1'}),
    ('query_et_complete_jobs',
     ['db', 'v1', datetime(2014, 07, 15)],
     {'start_ts': '2014/07/15', 'table_versions': 'v1'}),
    ('et_started',
     [{'data_date': 'date', 'table_versions': 'v1'}, 'db'],
     {'data_date': 'date', 'table_versions': 'v1'}),
    ('insert_et',
     [{'data_date': 'date', 'table_versions': 'v1'}, 'db'],
     {'data_date': 'date', 'et_status': 'started', 'table_versions': 'v1'}),
])
def test_rs_status_method(method_name, input_args, expected_args, psql):
    st = RedshiftStatusTable(psql)
    method = getattr(st, method_name)
    method(*input_args)
    called_args = psql.run_sql.call_args[1]['params']
    for k, v in expected_args.iteritems():
        assert called_args[k] == v


def test_rs_et_started_fail():
    st = RedshiftStatusTable(psql)
    with pytest.raises(ValueError):
        st.et_started(
            {
                'data_date': 12,
                'table_versions': 'v' * (VERSIONS_COL_SIZE+1)
            },
            'db'
        )


def data_date_from_today(days_back=0):
    now = datetime.utcnow().date()
    return datetime.strftime(now - timedelta(days=days_back), "%Y/%m/%d")


@pytest.mark.parametrize("method_name, input_args, expected_output", [
    ('log_status_result',
     [{'data_date': 'date', 'table_versions': 'v1'}, 5, 'db', False, None],
     None),
    ('update_status',
     ['db', 'date', 'v1', 'complete', 0, None],
     None),
    ('query_et_complete_job',
     ['db', 'v1', '2014/07/29'],
     [('2014/07/29', None)]),
    ('query_et_complete_jobs',
     ['db', 'v1', datetime.utcnow().date() - timedelta(days=2)],
     [(data_date_from_today(days_back=2), None),
      (data_date_from_today(days_back=1), None),
      (data_date_from_today(days_back=0), None)]),
    ('et_started',
     [{'data_date': 'date', 'table_versions': 'v1'}, 'db'],
     False),
    ('insert_et',
     [{'data_date': 'date', 'table_versions': 'v1'}, 'db'],
     None),
])
def test_dynamodb_status_method(method_name, input_args, expected_output):
    st = DynamoDbStatusTable(None)
    method = getattr(st, method_name)
    result = method(*input_args)
    assert result == expected_output
