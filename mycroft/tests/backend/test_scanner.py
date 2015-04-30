# -*- coding: utf-8 -*-
import datetime
from copy import copy
from datetime import timedelta
import mock
import pytest
from simplejson import dumps
import staticconf

from mycroft.backend.email import Mailer
from mycroft.backend.scanners.base_scanner import BaseScanner
from mycroft.backend.scanners.et_scanner import ETScanner
from tests.data.mock_config import MOCK_CONFIG
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_SCHEDULED
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_RUNNING
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_SUCCESS
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_EMPTY
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_PAUSED
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_ERROR
from tests.backend.test_worker import FakeSQS
from tests.data.scheduled_job import SCHEDULED_JOB_INPUT_DICT
from tests.data.scheduled_job import SCHEDULED_JOB_WITH_FUTURE_START_DATE
from tests.data.scheduled_job import SCHEDULED_JOB_INPUT_DICT_CUSTOM_TIME_LOG_AVAIL
from tests.data.scheduled_job import SAMPLE_RECORD_ET_STATUS_SCHEDULED
from tests.data.scheduled_job import SAMPLE_RECORD_ET_STATUS_RUNNING_1
from tests.data.scheduled_job import SAMPLE_RECORD_ET_STATUS_PAUSED
from tests.data.scheduled_job import SAMPLE_RECORD_ET_STATUS_ERROR
from tests.models.test_abstract_records import dynamodb_connection  # NOQA
from tests.models.test_scheduled_jobs import scheduled_jobs  # NOQA
from tests.models.test_scheduled_jobs import FakeScheduledJob


TODAY = datetime.datetime.utcnow().strftime("%Y-%m-%d")
YESTERDAY = (datetime.datetime.utcnow() - timedelta(days=1)).\
    strftime("%Y-%m-%d")
TWODAYSAGO = (datetime.datetime.utcnow() - timedelta(days=2)).\
    strftime("%Y-%m-%d")
# Yield fixtures for different types of scanners and tables


def fake_get_cluster_details(rs_id):
    return 'some-host', 1234, 'mycroft_namespace'


@pytest.yield_fixture  # noqa
def get_base_scanner(scheduled_jobs):
    db = scheduled_jobs
    scanner_sqs = FakeSQS()
    worker_sqs = FakeSQS()
    with staticconf.testing.MockConfiguration(MOCK_CONFIG):
        yield BaseScanner(db, scanner_sqs, worker_sqs, Mailer(True))


@pytest.yield_fixture  # noqa
def et_scanner_fixture(scheduled_jobs):
    db = scheduled_jobs
    scanner_sqs = FakeSQS()
    worker_sqs = FakeSQS()
    with staticconf.testing.MockConfiguration(MOCK_CONFIG):
        et_scanner = ETScanner(db, scanner_sqs, worker_sqs, Mailer(True))
        with mock.patch.object(
            et_scanner,
            '_get_redshift_cluster_details',
            autospec=True,
            return_value=('some-host', 1234, 'mycroft_namespace')
        ):
            yield et_scanner


@pytest.yield_fixture  # noqa
def set_rs_config():
    # TODO: Evaluate if this needs to be an yield fixture?
    config = {
        'default_mycroft_redshift_host': 'aa',
        'default_mycroft_redshift_port': 1234,
    }
    with staticconf.testing.MockConfiguration(config):
        yield


@pytest.fixture(scope="function")  # noqa
def mock_log_setup():
    with mock.patch(
        'mycroft.log_util.setup_pipeline_stream_logger',
        autospec=True
    ):
        return


class TestScanner(object):

    """
    Unit tests for :class:`mycroft.backend.scanner.base_scanner.Scanner`
    """

    def test_fetch_jobs_not_implemented(self, get_base_scanner):
        scanner = get_base_scanner
        with pytest.raises(NotImplementedError):
            scanner._fetch_jobs_for_work()

    def test_create_work_raises_notimplemented(self, get_base_scanner):
        scanner = get_base_scanner
        with pytest.raises(NotImplementedError):
            scanner._create_work_for_job(object())  # arg does not matter

    def test_run_scanner_raises_notimplemented(self, get_base_scanner):
        scanner = get_base_scanner
        with pytest.raises(NotImplementedError):
            scanner.run_scanner()

    def test_should_process_job_start_in_past(self, get_base_scanner):
        scanner = get_base_scanner
        entry = FakeScheduledJob(SCHEDULED_JOB_INPUT_DICT)
        assert scanner._should_process_job(entry)

    def test_should_process_job_start_in_future(self, get_base_scanner):
        scanner = get_base_scanner
        entry = FakeScheduledJob(SCHEDULED_JOB_WITH_FUTURE_START_DATE)
        assert not scanner._should_process_job(entry)

    def test_get_date_string(self, get_base_scanner):
        scanner = get_base_scanner
        yesterday_str = scanner._get_date_string(-1)
        yesterday = datetime.datetime.today() - timedelta(days=1)
        assert yesterday_str == yesterday.strftime("%Y-%m-%d")

    @pytest.mark.parametrize("date1, date2, result", [
        (None, None, None),
        ("2014-09-01", None, "2014-09-01"),
        ("2014-10-10", "2014-09-01", "2014-09-01")
    ])
    def test_earlier_date(self, get_base_scanner, date1, date2, result):
        scanner = get_base_scanner
        assert scanner._earlier_date(date1, date2) == result

    @pytest.mark.parametrize("date1, date2, result", [
        (None, None, None),
        ("2014-09-01", None, "2014-09-01"),
        ("2014-10-10", "2014-09-01", "2014-10-10")
    ])
    def test_later_date(self, get_base_scanner, date1, date2, result):
        scanner = get_base_scanner
        assert scanner._later_date(date1, date2) == result

    def test_data_available_for_date_unavailable_case(
            self, get_base_scanner):
        scanner = get_base_scanner
        row = copy(SCHEDULED_JOB_INPUT_DICT)
        row['time_log_need_to_be_available'] = '23:59'

        row_obj = FakeScheduledJob(row)
        ret = datetime.datetime(2014, 9, 1, 23, 0, 0)
        with mock.patch.object(
                scanner, '_get_utc_now', autospec=True, return_value=ret) as \
                mocked_time:
            assert not scanner._data_available_for_date(row_obj, '2014-09-01')
            assert mocked_time.call_count == 1

    def test_data_available_for_date_available_case(
            self, get_base_scanner):
        scanner = get_base_scanner
        row = copy(SCHEDULED_JOB_INPUT_DICT)
        row['time_log_need_to_be_available'] = '22:59'

        row_obj = FakeScheduledJob(row)
        ret = datetime.datetime(2014, 9, 3, 23, 0, 0)
        with mock.patch.object(
                scanner, '_get_utc_now', autospec=True, return_value=ret) as \
                mocked_time:
            assert scanner._data_available_for_date(row_obj, '2014-09-01')
            assert mocked_time.call_count == 1

    def test_enqueue_work_in_sqs(self, get_base_scanner):
        scanner = get_base_scanner

        scanner._get_redshift_cluster_details = fake_get_cluster_details
        config = {
            'aws_config': {'region': 'dummy_region',
                           'redshift_clusters': 'dummy_cluster_table'}
        }
        with staticconf.testing.MockConfiguration(config):
            job_dict = copy(SCHEDULED_JOB_INPUT_DICT)
            scanner._enqueue_work_in_sqs(
                "2014-09-09", "2015-09-19", "ET", job_dict)

            msg = scanner.worker_queue._published_msg
            # assert args we change/add
            assert msg is not None
            assert msg['script_start_date_arg'] == '2014-09-09'
            assert msg['script_end_date_arg'] == '2015-09-19'
            assert msg['job_type'] == "ET"
            assert msg['redshift_host'] is not None
            assert msg['redshift_port'] is not None
            assert msg['redshift_schema'] is not None
            actual_contact_emails = job_dict.pop(
                'contact_emails')
            assert set(msg['contact_emails']) == actual_contact_emails

            # assert original keys
            assert job_dict['additional_arguments'] == dumps(msg['additional_arguments'])
            del job_dict['additional_arguments']
            for key in job_dict:
                assert job_dict[key] == msg[key]

    @pytest.mark.parametrize("actions, result", [
        ({'cancel_requested': 0}, False),
        ({'cancel_requested': 1}, True),
        ({'cancel_requested': None}, False),
    ])
    def test_action_pending(self, et_scanner_fixture, actions, result):
        scanner = et_scanner_fixture
        row = FakeScheduledJob(SCHEDULED_JOB_INPUT_DICT)
        ret = row.update(**actions)
        assert ret is True
        assert scanner._action_pending(row) == result

    def test_max_complete_data_available(self, get_base_scanner):
        scanner = get_base_scanner
        row = copy(SCHEDULED_JOB_INPUT_DICT)
        row_obj = FakeScheduledJob(row)
        with mock.patch.object(
                scanner, '_get_max_complete_date', autospec=True, return_value='2015-03-20') as \
                mocked_get_max_complete_date:
            assert scanner._get_max_available_date(row_obj) == '2015-03-20'
            assert mocked_get_max_complete_date.call_count == 1

    def test_get_max_complete_date(self, get_base_scanner):
        row = copy(SCHEDULED_JOB_INPUT_DICT)
        row_obj = FakeScheduledJob(row)
        with mock.patch(
                'mycroft.backend.scanners.base_scanner.get_log_meta_data',
                autospec=True,
                return_value={'log': {'max_complete_date': '2015-03-15'}}
        ) as mocked_get_log_meta_data:
            assert get_base_scanner._get_max_available_date(row_obj) == '2015-03-15'
            assert mocked_get_log_meta_data.call_count == 1


class TestETScanner(object):

    """ Unit tests for
    :class:`mycroft.backend.scanner.et_scanner.ETScanner` class.
    """

    def test_fetch_jobs_with_entries_in_db(self, et_scanner_fixture):
        scanner = et_scanner_fixture

        scanner.db.put(**SCHEDULED_JOB_WITH_FUTURE_START_DATE)

        res = scanner._fetch_jobs_for_work()
        assert res is not None
        assert len(res) == 1

    @pytest.mark.parametrize("exception", [
        (Exception),
    ])
    def test_run_with_exceptions(self, et_scanner_fixture, exception):
        scanner = et_scanner_fixture
        scanner._run_once = True

        @mock.patch.object(scanner, '_get_timeout', autospec=True, return_value=1)
        @mock.patch.object(scanner.scanner_queue, 'get_queue_name', return_value='queue')
        @mock.patch.object(scanner.scanner_queue, 'get_wait_time', return_value=0)
        @mock.patch.object(scanner.scanner_queue, 'get_messages_from_queue', side_effect=exception)
        @mock.patch.object(scanner.scanner_queue, 'clear', autospec=True)
        @mock.patch('mycroft.backend.scanners.base_scanner.log_exception', autospec=True)
        def execute(*mocks):
            scanner.run()
            for m in mocks:
                assert m.call_count == 1
        execute()

    def test_run_scanner_no_exceptions(self, et_scanner_fixture):
        scanner = et_scanner_fixture

        row = copy(SCHEDULED_JOB_INPUT_DICT)
        row['hash_key'] = 'scanner_run_1'
        row['contact_emails'] = None
        row['start_date'] = '2014-09-01'
        row['et_status'] = 'null'
        row['load_status'] = 'null'
        scanner.db.put(**row)

        scanner.run_scanner()
        assert scanner.worker_queue._published_msg is not None

    def test_run_scanner_with_exception_not_thrown_out(self, et_scanner_fixture):
        scanner = et_scanner_fixture

        row = copy(SCHEDULED_JOB_WITH_FUTURE_START_DATE)
        row['hash_key'] = 'scanner_run_2'
        scanner.db.put(**row)

        with mock.patch.object(
                scanner, '_should_process_job', autospec=True,
                side_effect=ValueError('Some test error')) as mocked_scanner:
            scanner.run_scanner()
            assert mocked_scanner.call_count == 1
        assert scanner.worker_queue._published_msg is None

    @pytest.mark.parametrize("job_dict, result", [
        (SCHEDULED_JOB_INPUT_DICT, True),
        (SCHEDULED_JOB_WITH_FUTURE_START_DATE, False),
    ])
    def test_should_process_job(self, et_scanner_fixture, job_dict, result):
        scanner = et_scanner_fixture
        row = FakeScheduledJob(job_dict)
        assert scanner._should_process_job(row) == result

    @pytest.mark.parametrize("update_time, expected_status", [
        (datetime.datetime.utcnow() - timedelta(days=1), JOBS_ETL_STATUS_SCHEDULED),
        (datetime.datetime.utcnow() - timedelta(days=10), JOBS_ETL_STATUS_EMPTY),
    ])
    def test_maint_scheduled_jobs(self, et_scanner_fixture, update_time, expected_status):
        scanner = et_scanner_fixture
        hash_key = 'scheduled_job'
        row = copy(SAMPLE_RECORD_ET_STATUS_SCHEDULED)
        row['et_status_last_updated_at'] = str(update_time)
        row['hash_key'] = hash_key
        scanner.db.put(**row)
        scanner._maint_scheduled_jobs(scanner._get_utc_now())
        job = scanner.db.get(hash_key=hash_key)
        assert job.get(et_status=None)['et_status'] == expected_status

    @pytest.mark.parametrize("update_time, expected_status", [
        (datetime.datetime.utcnow() - timedelta(seconds=1), JOBS_ETL_STATUS_RUNNING),
        (datetime.datetime.utcnow() - timedelta(seconds=10*60), JOBS_ETL_STATUS_EMPTY),
    ])
    def test_maint_running_jobs(self, et_scanner_fixture, update_time, expected_status):
        scanner = et_scanner_fixture
        hash_key = 'running_job'
        row = copy(SAMPLE_RECORD_ET_STATUS_RUNNING_1)
        row['et_status_last_updated_at'] = str(update_time)
        row['hash_key'] = hash_key
        scanner.db.put(**row)
        scanner._maint_running_jobs(scanner._get_utc_now())
        job = scanner.db.get(hash_key=hash_key)
        assert job.get(et_status=None)['et_status'] == expected_status

    @pytest.mark.parametrize("next_retry, retry_count, expected_status", [
        # legacy job (no retry count), skip job
        (datetime.datetime.utcnow() - timedelta(seconds=60), None, JOBS_ETL_STATUS_ERROR),
        # retry should proceed
        (datetime.datetime.utcnow() - timedelta(seconds=60), 0, JOBS_ETL_STATUS_EMPTY),
        # exhausted max retries, skip job
        (datetime.datetime.utcnow() - timedelta(seconds=60), 1, JOBS_ETL_STATUS_ERROR),
        # next retry is in the future, skip job
        (datetime.datetime.utcnow() + timedelta(seconds=60), 0, JOBS_ETL_STATUS_ERROR),
    ])
    def test_maint_error_jobs(
            self, et_scanner_fixture, next_retry, retry_count, expected_status
    ):
        scanner = et_scanner_fixture
        hash_key = 'error_job'
        row = copy(SAMPLE_RECORD_ET_STATUS_ERROR)
        if retry_count is not None:
            row['et_num_error_retries'] = retry_count
        row['et_next_error_retry_attempt'] = str(next_retry)
        row['hash_key'] = hash_key
        scanner.db.put(**row)
        scanner._maint_error_jobs(scanner._get_utc_now())
        job = scanner.db.get(hash_key=hash_key)
        assert job.get(et_status=None)['et_status'] == expected_status

    @pytest.mark.parametrize("pause_requested, cancel_requested, lsd, expected_status", [
        (None, None, None, JOBS_ETL_STATUS_EMPTY),
        (0, None, None, JOBS_ETL_STATUS_EMPTY),
        (0, None, TWODAYSAGO, JOBS_ETL_STATUS_SUCCESS),
        (1, None, None, JOBS_ETL_STATUS_PAUSED),
        (0, 1, None, JOBS_ETL_STATUS_EMPTY),
        (1, 1, None, JOBS_ETL_STATUS_EMPTY),
    ])
    def test_maint_paused_jobs(
        self, et_scanner_fixture, pause_requested, cancel_requested, lsd, expected_status
    ):
        scanner = et_scanner_fixture
        hash_key = 'paused_job'
        row = copy(SAMPLE_RECORD_ET_STATUS_PAUSED)
        row['pause_requested'] = pause_requested
        row['cancel_requested'] = cancel_requested
        row['et_last_successful_date'] = lsd
        row['hash_key'] = hash_key
        scanner.db.put(**row)
        with mock.patch.object(scanner.emailer, "mail_result", autospec=True):
            scanner._maint_paused_jobs(scanner._get_utc_now())
        job = scanner.db.get(hash_key=hash_key)
        assert job.get(et_status=None)['et_status'] == expected_status

    @pytest.mark.parametrize("cancel_requested, pause_requested, expected_status", [
        (False, False, JOBS_ETL_STATUS_ERROR),
        (False, True, JOBS_ETL_STATUS_EMPTY),
        (True, False, JOBS_ETL_STATUS_EMPTY),
        (True, True, JOBS_ETL_STATUS_EMPTY),
    ])
    def test_maint_error_jobs_action_pending(
        self, et_scanner_fixture, cancel_requested, pause_requested, expected_status
    ):
        scanner = et_scanner_fixture
        hash_key = 'error_job'
        row = copy(SAMPLE_RECORD_ET_STATUS_ERROR)
        row['cancel_requested'] = cancel_requested
        row['pause_requested'] = pause_requested
        row['hash_key'] = hash_key
        scanner.db.put(**row)
        scanner._maint_error_jobs(scanner._get_utc_now())
        job = scanner.db.get(hash_key=hash_key)
        assert job.get(et_status=None)['et_status'] == expected_status

    @pytest.mark.parametrize("last_et_date, availability_time, result", [
        (YESTERDAY, "00:00", True),
        (YESTERDAY, "23:59", False),
        (YESTERDAY, None, False)
    ])
    def test_should_process_job_data_available_now(
            self, et_scanner_fixture, last_et_date, availability_time, result):
        scanner = et_scanner_fixture
        row = copy(SCHEDULED_JOB_INPUT_DICT)
        row['et_last_successful_date'] = last_et_date
        if availability_time is not None:
            row['time_log_need_to_be_available'] = availability_time
        row = FakeScheduledJob(row)

        assert scanner._should_process_job(row) == result

    def test_update_job_status(self, et_scanner_fixture):
        # Simple test for now
        scanner = et_scanner_fixture
        row = FakeScheduledJob(SCHEDULED_JOB_INPUT_DICT)
        assert scanner._update_job_status_conditionally(row)
        assert row.get(et_status=None)['et_status'] == JOBS_ETL_STATUS_SCHEDULED

    def test_create_work_update_failed(self, et_scanner_fixture):
        scanner = et_scanner_fixture
        row = FakeScheduledJob(SCHEDULED_JOB_INPUT_DICT)
        with mock.patch.object(
                scanner, '_update_job_status_conditionally',
                autospec=True, return_value=False) as mocked_scanner:
            scanner._create_work_for_job(row)
            assert mocked_scanner.call_count == 1

        assert scanner.worker_queue._published_msg is None

    @pytest.mark.parametrize(
        "start_date, end_date, et_status, last_et, res_start, res_end",
        [
            ("2014-01-01", "2014-01-10", "null", None, "2014-01-01",
                "2014-01-10"),
            ("2014-01-01", None, "null", None, "2014-01-01", TWODAYSAGO)
        ]
    )
    def test_create_work(
            self, et_scanner_fixture, start_date, end_date,
            et_status, last_et, res_start, res_end):
        scanner = et_scanner_fixture
        row = copy(SCHEDULED_JOB_INPUT_DICT)
        row['start_date'] = start_date
        row['end_date'] = end_date
        row['et_status'] = et_status
        row['et_last_successful_date'] = last_et
        row_dbobj = FakeScheduledJob(row)

        scanner._create_work_for_job(row_dbobj)
        msg = scanner.worker_queue._published_msg
        assert msg is not None
        assert msg['start_date'] == row['start_date']
        assert msg['end_date'] == row['end_date']
        assert msg['job_type'] == 'ET'
        assert msg['script_start_date_arg'] == res_start
        assert msg['script_end_date_arg'] == res_end

    def test_create_work_job_range_current_run_till_yesterday_data_unavail_now(
            self, et_scanner_fixture):
        scanner = et_scanner_fixture
        row = copy(SCHEDULED_JOB_INPUT_DICT)
        row['start_date'] = '2014-09-01'
        row['end_date'] = None
        row['et_status'] = 'null'
        yesterday = (datetime.datetime.utcnow() - timedelta(days=1))\
            .strftime("%Y-%m-%d")
        row['et_last_successful_date'] = '2014-09-03'
        next_min = (datetime.datetime.utcnow() + timedelta(minutes=1)).\
            strftime("%H:%M")
        row['time_log_need_to_be_available'] = next_min
        row = FakeScheduledJob(row)

        scanner._create_work_for_job(row)
        msg = scanner.worker_queue._published_msg
        assert msg is not None
        assert msg['script_start_date_arg'] == '2014-09-04'  # et_last_success plus 1
        assert msg['script_end_date_arg'] == yesterday  # today's data is not available
        assert msg['job_type'] == 'ET'

    def test_time_log_need_to_be_avail(self, get_base_scanner):
        scanner = get_base_scanner
        entry = FakeScheduledJob(SCHEDULED_JOB_INPUT_DICT)
        assert scanner._get_time_log_need_to_be_available(entry) == '48:00'

    def test_time_log_need_to_be_avail_from_additional_arguments(self, get_base_scanner):
        scanner = get_base_scanner
        entry = FakeScheduledJob(SCHEDULED_JOB_INPUT_DICT_CUSTOM_TIME_LOG_AVAIL)
        assert scanner._get_time_log_need_to_be_available(entry) == '12:30'
