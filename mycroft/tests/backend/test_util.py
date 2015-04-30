import pytest
from mycroft.backend.util import next_date_for_string
from mycroft.backend.util import parse_results
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_SUCCESS as SUCCESS
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_ERROR as ERROR


@pytest.mark.parametrize("date, result", [
    (None, None),
    ("2014-08-01", "2014-08-02")
])
def test_next_date_for_string(date, result):
    assert next_date_for_string(date) == result


RECORDS_NONE = {}
RECORDS_ALL_SUCCESS = {
    '2014-09-01': [{'status': 'success'}],
    '2014-09-02': [{'status': 'success'}],
}
RECORDS_LAST_ONE_FAILED = {
    '2014-09-30': [{'status': 'success'}],
    '2014-10-01': [{'status': 'error'}],
}
RECORDS_INTERMEDIATE_ONE_FAILED = {
    '2014-09-30': [{'status': 'success'}],
    '2014-10-01': [{'status': 'error'}],
    '2014-10-02': [{'status': 'success'}],
}
RECORDS_LAST_STEP_FAILED = {
    '2014-09-30': [{'status': 'success'}],
    '2014-10-01': [
        {'status': 'success', 'step': 'et'},
        {'status': 'error', 'step': 'load'},
    ],
}


@pytest.mark.parametrize("records, res_status, res_lsd", [
    (RECORDS_NONE, ERROR, None),
    (RECORDS_ALL_SUCCESS, SUCCESS, '2014-09-02'),
    (RECORDS_LAST_ONE_FAILED, ERROR, '2014-09-30'),
    (RECORDS_INTERMEDIATE_ONE_FAILED, ERROR, '2014-09-30'),
    (RECORDS_LAST_STEP_FAILED, ERROR, '2014-09-30'),
])
def test_parse_results(records, res_status, res_lsd):
    status, lsd, _ = parse_results(records)
    assert status == res_status
    assert lsd == res_lsd
