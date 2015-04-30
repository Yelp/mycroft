# -*- coding: utf-8 -*-
import os
from copy import copy

import mock
import pytest

import staticconf.testing
from boto.sqs.jsonmessage import JSONMessage
from mycroft.backend.email import Mailer
from mycroft.backend.sqs_wrapper import SQSWrapper
from mycroft.backend.worker.base_worker import setup_config
from mycroft.backend.worker.et_worker import parse_cmd_args
from mycroft.backend.worker.base_worker import WorkerJob
from mycroft.backend.worker.base_worker import BaseMycroftWorker
from mycroft.backend.worker.et_worker import ImdWorker
from mycroft.backend.util import datetime_to_date_string
from mycroft.backend.util import date_string_total_items
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_RUNNING
from tests.data.mock_config import MOCK_CONFIG
import argparse
from staticconf import read_string
from tests.data.scheduled_job import SAMPLE_RECORD_ET_STATUS_RUNNING_1
from tests.data.scheduled_job import SAMPLE_RECORD_ET_STATUS_SCHEDULED
from tests.models.test_abstract_records import dynamodb_connection  # noqa
from tests.models.test_etl_record import etl_records  # noqa
from tests.models.test_scheduled_jobs import scheduled_jobs  # noqa


class FakeSQS(SQSWrapper):

    def __init__(self, msg_list=None):
        self._published_msg = None
        self.msgs = msg_list

    def get_messages_from_queue(self):
        return self.msgs

    def write_message_to_queue(self, msg):
        self._published_msg = msg

    def delete_message_from_queue(self, msg):
        pass

    def get_queue_attributes(self):
        return {'MessageRetentionPeriod': 4*24*3600}

    def get_wait_time(self):
        return 0

SAMPLE_JSON_SQS_MSG = JSONMessage(body={
    'start_date': '2014-02-02',
    'end_date': '2014-02-06',
    'script_start_date_arg': '2014-02-02',
    'script_end_date_arg':   '2014-02-06',
    'step': 1,
    'log_name': 'log',
    'log_schema_version': 'version',
    'hash_key': SAMPLE_RECORD_ET_STATUS_SCHEDULED['hash_key'],
    'redshift_id': 'some-id',
    'redshift_host': 'host',
    'redshift_port': 'port',
    'redshift_schema': 'rs_namespace',
    's3_path': 's3_path',
    'uuid': 'uuid',
    'additional_arguments': {},
})

SUCCESS_RECORD = {
    'status': 'success', 'start_time': 4, 'end_time': 5, 'date': '2014-02-02', 'error_info': {}
}

ERROR_RECORD = {
    'status': 'error', 'start_time': 4, 'end_time': 5, 'date': '2014-02-02',
    'error_info': {'crash_a': 'error_a', 'crash_b': 'error_b'}
}

CONFIG_LOC = os.path.join(os.getcwd(), "config.yaml")

CONFIG_OVERRIDE_LOC = os.path.join(os.getcwd(), "config-env-dev.yaml")


# Yield fixtures for different types of workers
@pytest.yield_fixture(params=['et'])  # noqa
def get_worker(request, scheduled_jobs, etl_records):
    with staticconf.testing.MockConfiguration(MOCK_CONFIG):
        with mock.patch(
                'mycroft.models.aws_connections.TableConnection.get_connection'
                ) as mocked_tc:
            scheduled_jobs.put(**SAMPLE_RECORD_ET_STATUS_SCHEDULED)
            mocked_tc.side_effect = lambda x: {
                'ScheduledJobs': scheduled_jobs, 'ETLRecords': etl_records
            }[x]
            workers = {
                'et':   ImdWorker(CONFIG_LOC, CONFIG_OVERRIDE_LOC, True, Mailer(True)),
            }
            yield workers[request.param]


class TestMycroftWorker(object):

    def setup_run_test(self, get_worker):
        self.sqs = FakeSQS([SAMPLE_JSON_SQS_MSG])
        worker = get_worker
        worker._run_once = True
        return worker

    def test_run(self, get_worker):
        worker = self.setup_run_test(get_worker)

        @mock.patch.object(worker, '_get_sqs_wrapper', return_value=self.sqs, autospec=True)
        @mock.patch.object(worker, '_update_scheduled_jobs', autospec=True)
        @mock.patch.object(worker, '_process_msg', autospec=True, return_value=(
            {'2014-01-01': [{'date': '2014-01-01', 'status': 'success'}]},
            {'cancel_requested': False, 'pause_requested': False}
        ))
        @mock.patch.object(worker.emailer, 'mail_result', autospec=True)
        def execute(*mocks):
            worker.run()
        execute()

    @pytest.mark.parametrize("action_results, expected_status", [
        ({'cancel_requested': True, 'pause_requested': False,
          'delete_requested': False}, 'cancelled'),
        ({'cancel_requested': False, 'pause_requested': True,
          'delete_requested': False}, 'paused'),
        ({'cancel_requested': True, 'pause_requested': True,
          'delete_requested': False}, 'cancelled'),
    ])
    def test_run_with_cancel_and_pause(self, get_worker, action_results, expected_status):
        worker = self.setup_run_test(get_worker)

        @mock.patch.object(worker, '_get_sqs_wrapper', return_value=self.sqs, autospec=True)
        @mock.patch.object(worker, '_update_scheduled_jobs', autospec=True)
        @mock.patch.object(worker, '_process_msg', autospec=True, return_value=(
            {'2014-01-01': [{'date': '2014-01-01', 'status': 'success'}]},
            action_results
        ))
        @mock.patch.object(worker.emailer, 'mail_result', autospec=True)
        def execute(*mocks):
            worker.run()
            # make sure that _update_scheduled_jobs was called with status paused
            assert mocks[2].mock_calls[1][1][1]['et_status'] == expected_status
        execute()

    def test_run_stop(self, get_worker):
        worker = self.setup_run_test(get_worker)

        @mock.patch.object(worker, '_get_sqs_wrapper', return_value=self.sqs, autospec=True)
        def execute(*mocks):
            with mock.patch.object(
                self.sqs, 'get_messages_from_queue', autospec=True
            ) as mock_sqs_call:
                worker.stop()
                worker.run()
                assert mock_sqs_call.call_args is None
        execute()

    def test_run_sqs_exception(self, get_worker):
        worker = self.setup_run_test(get_worker)

        @mock.patch.object(worker, '_get_sqs_wrapper', return_value=self.sqs, autospec=True)
        @mock.patch.object(
            self.sqs, 'get_messages_from_queue', side_effect=Exception(), autospec=True
        )
        def execute(*mocks):
            with mock.patch(
                'mycroft.backend.worker.base_worker.log_exception',
                autospec=True
            ) as mock_log:
                worker.run()
                assert mock_log.call_args is not None
        execute()

    def test_worker_dummy_run(self, get_worker):
        """
        Unit tests for :class:`mycroft.backend.worker.base_worker.MycroftWorker`
        """
        worker = get_worker
        dummy_worker = ImdWorker('', '', True, Mailer(True), dummy_run=True)
        assert worker.dummy_run is False
        assert dummy_worker.dummy_run is True

    def test_run_update_exception(self, get_worker):
        worker = self.setup_run_test(get_worker)

        @mock.patch.object(worker, '_get_sqs_wrapper', return_value=self.sqs, autospec=True)
        @mock.patch.object(
            worker, '_update_scheduled_jobs', side_effect=Exception(), autospec=True
        )
        def execute(*mocks):
            with mock.patch(
                'mycroft.backend.worker.base_worker.log_exception',
                autospec=True
            ) as mock_log:
                worker.run()
                assert mock_log.call_args is not None
        execute()

    @pytest.mark.parametrize("job_args, results", [
        (({}, 0), []),
        (({}, 0), [{'date': 'date', 'status': 'error'}]),
        (({}, -1), [{'date': 'date', 'status': 'error'}]),
    ])
    def test__run_complete_callback_with_exceptions(self, get_worker, job_args, results):
        worker = get_worker
        job = WorkerJob(worker, *job_args)
        # callback can't raise exceptions without making et_pool library go nuts
        worker._run_complete_callback(job, 'id', 'func_name', results)

    def test_run_results_exception(self, get_worker):
        worker = self.setup_run_test(get_worker)

        @mock.patch.object(worker, '_get_sqs_wrapper', return_value=self.sqs, autospec=True)
        @mock.patch.object(worker, '_update_scheduled_jobs', autospec=True)
        @mock.patch.object(worker, '_process_msg', side_effect=Exception(), autospec=True)
        @mock.patch.object(worker.emailer, 'mail_result', autospec=True)
        def execute(*mocks):
            with mock.patch(
                'mycroft.backend.worker.base_worker.log_exception',
                autospec=True
            ) as mock_log:
                worker.run()
                assert mock_log.call_args is not None
        execute()

    @pytest.mark.parametrize("action_req", [
        ({'cancel_requested': True, 'pause_requested': False, 'delete_requested': False}),
        ({'cancel_requested': False, 'pause_requested': True, 'delete_requested': False}),
        ({'cancel_requested': True, 'pause_requested': True, 'delete_requested': False}),
    ])
    def test__process_msg(self, get_worker, action_req):
        worker = get_worker
        with mock.patch(
            'mycroft.backend.worker.base_worker.PoolExtended',
            autospec=True,
        ) as mock_pool:

            scheduled_runs = set()

            def invoke_callback(wait_sec):
                # wait releases lock and so do we
                worker._cond.release()

                # mock object stores all calls to apply_async from start
                # of test.  We only want to track calls made since latest
                # invokation of _process_msg.  We do this by tracking calls
                # via scheduled_runs set variable.
                all_apply_async_calls = [c for c in mock_pool.mock_calls
                                         if c[0] == '().apply_async']
                assert len(all_apply_async_calls) > 0
                for c in reversed(all_apply_async_calls):
                    kwargs = c[2]
                    if str(kwargs) in scheduled_runs:
                        break
                    scheduled_runs.add(str(kwargs))

                    date = datetime_to_date_string(kwargs['args'][0].start_date)
                    kwargs['callback']([
                        dict(SUCCESS_RECORD.items() + {'date': date}.items())
                    ])

                # acquire lock when resume from wait
                worker._cond.acquire()

            with mock.patch.object(worker._cond, 'wait',
                                   side_effect=invoke_callback, autospec=True):
                msg_dict = SAMPLE_JSON_SQS_MSG.get_body()
                start = msg_dict['script_start_date_arg']
                end = msg_dict['script_end_date_arg']
                step = msg_dict['step']
                results_expected = date_string_total_items(start, end, step=step)
                results, _ = worker._process_msg(SAMPLE_JSON_SQS_MSG)
                assert len(results) == results_expected

                # test cancel
                class TestJob(WorkerJob):
                    def update_action_requests(self):
                        self.actions = action_req

                with mock.patch.object(worker, 'create_worker_job',
                                       return_value=TestJob(worker, {}, worker._num_processes),
                                       autospec=True):
                    result, action_results = worker._process_msg(SAMPLE_JSON_SQS_MSG)
                    # we use 1 for True due to backend storage issues
                    assert action_results['cancel_requested'] == action_req['cancel_requested']
                    assert action_results['pause_requested'] == action_req['pause_requested']

    def test_setup_config_no_exceptions(self):
        args = argparse.Namespace(
            config=None, config_override=None, run_local=False
        )
        with staticconf.testing.MockConfiguration(MOCK_CONFIG):
            setup_config(args, 'test_worker')

    def test_setup_config_with_env_vars(self):
        args = parse_cmd_args(['program', '--config=./config.yaml',
                               '--config-override=config-env-dev.yaml', '-r'])
        with staticconf.testing.MockConfiguration(MOCK_CONFIG):
            setup_config(args, 'test_worker')
            # pick some key and ensure it ws loaded from config
            assert read_string('log_stream_name', 'default') != 'default'

    @pytest.mark.parametrize("method_name, arg_list", [
        ('has_more_runs_to_schedule', []),
        ('schedule_next_run', []),
        ('run_complete', [None, None, []]),
    ])
    def test_worker_job_raises_notimplemented(self, method_name, arg_list):
            job = WorkerJob(None, {}, 0)
            with pytest.raises(NotImplementedError):
                method = getattr(job, method_name)
                method(*arg_list) if len(arg_list) > 0 else method()

    @pytest.mark.parametrize("method_name, arg_list", [
        ('create_worker_job', [{}]),
        ('_get_queue_name', []),
        ('_get_scanner_queue_name', []),
    ])
    def test_worker_raises_notimplemented(self, method_name, arg_list):
        with mock.patch(
            'mycroft.models.aws_connections.TableConnection.get_connection'
        ):
            with staticconf.testing.MockConfiguration(MOCK_CONFIG):
                worker = BaseMycroftWorker('', '', '')
                with pytest.raises(NotImplementedError):
                    method = getattr(worker, method_name)
                    method(*arg_list) if len(arg_list) > 0 else method()

    @pytest.mark.parametrize("status, lsd, res_status, res_lsd", [
        ('success', '2014-09-01', 'success', '2014-09-01'),
        ('complete', '2014-09-02', 'complete', '2014-09-02'),
        ('error', '2014-09-01', 'error', '2014-09-01'),
        ('error', None, 'error', None),
    ])
    def test_update_scheduled_jobs_with_existing_job_entry(
            self, get_worker, status, lsd, res_status, res_lsd):
        worker = get_worker
        msg_dict = {
            'hash_key': SAMPLE_RECORD_ET_STATUS_RUNNING_1['hash_key'],
            'end_date': '2014-09-02',
        }
        kwargs = {
            'et_status': None,
            'et_last_successful_date': None,
        }
        worker._update_scheduled_jobs_on_etl_complete(msg_dict, status, lsd)
        job = worker.jobs_db.get(hash_key=msg_dict['hash_key'])
        assert job is not None
        job_dict = job.get(**kwargs)
        assert job_dict['et_status'] == res_status
        assert job_dict['et_last_successful_date'] == res_lsd

    def test_update_scheduled_jobs_without_job_entry(self, get_worker):
        worker = get_worker
        msg_dict = {
            'hash_key': 'a-non-existent-key',
            'end_date': None,
            'script_end_date_arg': '2014-09-01',
        }
        with pytest.raises(KeyError):
            worker._update_scheduled_jobs_on_etl_complete(
                msg_dict, 'success', None
            )

    def test_update_scheduled_jobs_on_etl_start(self, get_worker):
        worker = get_worker
        msg_dict_ok = {
            'hash_key': SAMPLE_RECORD_ET_STATUS_SCHEDULED['hash_key'],
            'end_date': '2014-09-02',
        }

        worker._update_scheduled_jobs_on_etl_start(msg_dict_ok)
        job = worker.jobs_db.get(hash_key=msg_dict_ok['hash_key'])
        assert job is not None
        kwargs = {'et_status': None}
        job_dict = job.get(**kwargs)
        assert job_dict['et_status'] == JOBS_ETL_STATUS_RUNNING

        msg_dict_error = {
            'hash_key': SAMPLE_RECORD_ET_STATUS_RUNNING_1['hash_key'],
            'end_date': '2014-09-02',
        }
        with pytest.raises(ValueError):
            worker._update_scheduled_jobs_on_etl_start(msg_dict_error)

    def test_update_action_requests_for_job_with_exception(self, get_worker):
        worker = get_worker
        with mock.patch.object(worker.jobs_db, 'get', return_value=None, autospec=True):
            with pytest.raises(ValueError):
                msg_dict = copy(SAMPLE_RECORD_ET_STATUS_RUNNING_1)
                msg_dict['script_start_date_arg'] = '2014-10-01'
                msg_dict['script_end_date_arg'] = '2014-10-01'
                job = worker.create_worker_job(msg_dict)
                job.update_action_requests()

    def test__update_scheduled_jobs_exceptions(self, get_worker):
        worker = get_worker
        with mock.patch.object(worker.jobs_db, 'get', return_value=None, autospec=True):
            with pytest.raises(ValueError):
                worker._update_scheduled_jobs(
                    SAMPLE_RECORD_ET_STATUS_RUNNING_1['hash_key'], {}
                )

        class FakeScheduledJob(object):
            def update(args):
                return None

        with mock.patch.object(worker.jobs_db, 'get',
                               return_value=FakeScheduledJob(), autospec=True):
            with pytest.raises(ValueError):
                worker._update_scheduled_jobs('', {})
