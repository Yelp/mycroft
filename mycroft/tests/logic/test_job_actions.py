# -*- coding: utf-8 -*-
import mock
import pytest
import simplejson
from simplejson import JSONDecodeError
from copy import copy
from boto.dynamodb2.exceptions import ItemNotFound

from mycroft.logic.job_actions import _create_hash_key
from mycroft.logic.job_actions import _parse_jobs
from mycroft.logic.job_actions import list_all_jobs
from mycroft.logic.job_actions import list_jobs_by_name
from mycroft.logic.job_actions import list_jobs_by_name_version
from mycroft.logic.job_actions import post_job
from mycroft.logic.job_actions import put_job
from mycroft.logic.job_actions import ACTION_TO_REQUIRED_ARGS
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUSES_FINAL
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_SCHEDULED
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_RUNNING

from tests.data.scheduled_job import SAMPLE_LOG_NAME
from tests.data.scheduled_job import SAMPLE_LOG_VERSION
from tests.data.scheduled_job import SCHEDULED_JOB_INPUT_DICT
from tests.data.scheduled_job import SAMPLE_SCHEDULED_JOBS
from tests.models.test_abstract_records import dynamodb_connection  # noqa
from tests.models.test_scheduled_jobs import TestScheduledJobs as TstScheduledJobs
from tests.backend.test_worker import FakeSQS

from mycroft.models.abstract_records import PrimaryKeyError


BASE_DICT = {
    'log_name': None,
    'log_schema_version': None,
    's3_path': None,
    'start_date': None,
    'end_date': None,
    'contact_emails': [],
    'redshift_id': None,
    'uuid': None,
    'et_status': None,
    'load_status': None,
    'cancel_requested': None,
    'pause_requested': None,
    'delete_requested': None,
    'additional_arguments': None
}


class TestJobActions(object):

    def post_job(self, scheduled_jobs, et_scanner_queue, input_string):
        with mock.patch('mycroft.models.aws_connections.TableConnection.get_connection'):
            with mock.patch('mycroft.logic.job_actions.list_cluster_by_name'):
                result = post_job(scheduled_jobs, et_scanner_queue, input_string)
                return result

    @pytest.yield_fixture(scope='function')  # noqa
    def scheduled_jobs(self, dynamodb_connection):
        tsj = TstScheduledJobs()
        table, sj = tsj._get_scheduled_jobs(dynamodb_connection)
        for job in SAMPLE_SCHEDULED_JOBS:
            assert sj.put(**job)
        yield sj
        assert table.delete()

    def test__parse_jobs_empty_job(self):
        empty_jobs = [TstScheduledJobs().create_fake_scheduled_job(BASE_DICT)]
        result = _parse_jobs(empty_jobs)
        assert result['jobs'][0] == BASE_DICT

    def test_list_jobs_by_name(self, scheduled_jobs):
        return_value = list_jobs_by_name(SAMPLE_LOG_NAME, scheduled_jobs)
        expected_count = len([job for job in SAMPLE_SCHEDULED_JOBS
                              if job['log_name'] == SAMPLE_LOG_NAME])
        assert len(return_value['jobs']) == expected_count

    @pytest.mark.parametrize("log_name", [':', None, '..', 'x_!x', 'x!', '!x'])
    def test_list_jobs_by_name_bad_input(self, log_name):
        with pytest.raises(ValueError) as e:
            list_jobs_by_name(log_name, None)
        assert e.exconly() == 'ValueError: invalid log_name'

    def test_list_jobs_by_name_version(self, scheduled_jobs):
        return_value = list_jobs_by_name_version('ad_click', 'initial', scheduled_jobs)
        expected_count = len([job for job in SAMPLE_SCHEDULED_JOBS
                              if job['log_name'] == SAMPLE_LOG_NAME
                              and job['log_schema_version'] == SAMPLE_LOG_VERSION])
        assert len(return_value['jobs']) == expected_count

    @pytest.mark.parametrize("log_name, log_version", [(None, 'y'), ('..', 'y'), ('', 'y')])
    def test_list_jobs_by_name_bad_log_name(self, log_name, log_version):
        with pytest.raises(ValueError) as e:
            list_jobs_by_name_version(log_name, log_version, None)
        assert e.exconly() == 'ValueError: invalid log_name'

    @pytest.mark.parametrize("log_name, log_version", [('x', None), ('x', '.'), ('x', '')])
    def test_list_jobs_by_name_bad_log_version(self, log_name, log_version):
        with pytest.raises(ValueError) as e:
            list_jobs_by_name_version(log_name, log_version, None)
        assert e.exconly() == 'ValueError: invalid log_version'

    def test_list_all_jobs(self, scheduled_jobs):
        assert len(list_all_jobs(scheduled_jobs)['jobs']) == len(SAMPLE_SCHEDULED_JOBS)

    def test__create_hash_key(self):
        result = _create_hash_key(SCHEDULED_JOB_INPUT_DICT)
        assert result == 'rs1:user_test:third:2014-08-15:None'

    @pytest.mark.parametrize("missing_key",
                             ['redshift_id', 'log_name',
                              'log_schema_version', 'start_date'])
    def test_create_insufficient_key(self, missing_key):
        test_dict = dict(SCHEDULED_JOB_INPUT_DICT)
        test_dict.pop(missing_key)
        with pytest.raises(PrimaryKeyError):
            _create_hash_key(test_dict)

    @pytest.fixture(scope='function')
    def get_json_happy_dict(self):
        """ Returns a JSON serializable job dictionary
        """
        job_dict = copy(SCHEDULED_JOB_INPUT_DICT)
        job_dict['contact_emails'] = list(job_dict['contact_emails'])
        return job_dict

    def test_post_acceptable_job(self, scheduled_jobs, get_json_happy_dict):
        input_string = simplejson.dumps(get_json_happy_dict)
        et_scanner_queue = FakeSQS()

        result = self.post_job(scheduled_jobs, et_scanner_queue, input_string)
        assert 'post_accepted' in result
        assert result['post_accepted']['result'] is True
        assert et_scanner_queue._published_msg == {'message': 'dummy'}

        with mock.patch('mycroft.models.aws_connections.TableConnection.get_connection'):
            with mock.patch(
                'mycroft.logic.job_actions.list_cluster_by_name',
                side_effect=ItemNotFound()
            ):
                with pytest.raises(ItemNotFound):
                    result = post_job(scheduled_jobs, et_scanner_queue, input_string)

    def test_post_duplicate_job(self, scheduled_jobs, get_json_happy_dict):
        input_string = simplejson.dumps(get_json_happy_dict)
        et_scanner_queue = FakeSQS()
        result = self.post_job(scheduled_jobs, et_scanner_queue, input_string)
        assert 'post_accepted' in result
        assert result['post_accepted']['result'] is True
        assert et_scanner_queue._published_msg == {'message': 'dummy'}
        with pytest.raises(ValueError):
            self.post_job(scheduled_jobs, et_scanner_queue, input_string)

    @pytest.mark.parametrize("missing_key",
                             ['log_name', 'contact_emails',
                              'log_schema_version', 'start_date'])
    def test_post_insufficient_key(self, missing_key, get_json_happy_dict):
        test_dict = dict(get_json_happy_dict)
        test_dict.pop(missing_key)
        input_string = simplejson.dumps(test_dict)
        with pytest.raises(PrimaryKeyError):
            self.post_job(None, None, input_string)

    @pytest.mark.parametrize("bad_start_date, bad_end_date",
                             [("asdf", None), ("asdf", "asdf"),
                              ("2014-12-13", "asdf"), ("2014-12-13", "2014-12-12")])
    def test_post_invalid_job(self, bad_start_date, bad_end_date, get_json_happy_dict):
        test_dict = dict(get_json_happy_dict)
        test_dict['start_date'] = bad_start_date
        test_dict['end_date'] = bad_end_date
        input_string = simplejson.dumps(test_dict)
        with pytest.raises(ValueError):
            self.post_job(None, None, input_string)

    def test_post_no_kwargs(self):
        with pytest.raises(JSONDecodeError):
            self.post_job(None, None, "")

    def test_put_nonexisting_job(self, scheduled_jobs, get_json_happy_dict):
        input_string = simplejson.dumps(get_json_happy_dict)
        et_scanner_queue = FakeSQS()
        with pytest.raises(KeyError):
            put_job(scheduled_jobs, et_scanner_queue, input_string)

    def test_put_acceptable_job(self, scheduled_jobs, get_json_happy_dict):
        # post a new job
        input_string = simplejson.dumps(get_json_happy_dict)
        et_scanner_queue = FakeSQS()
        result = self.post_job(scheduled_jobs, et_scanner_queue, input_string)
        # update the both cancel_requested and pause_requested of the job
        required_args = list(ACTION_TO_REQUIRED_ARGS['put'])
        new_happy_dict = dict(zip(required_args, [get_json_happy_dict[k] for k in required_args]))
        new_happy_dict['cancel_requested'] = True
        new_happy_dict['pause_requested'] = True
        input_string = simplejson.dumps(new_happy_dict)
        result = put_job(scheduled_jobs, et_scanner_queue, input_string)
        assert 'put_accepted' in result
        assert result['put_accepted'] is True
        assert et_scanner_queue._published_msg == {'message': 'job put request'}

    @pytest.mark.parametrize("job_status", JOBS_ETL_STATUSES_FINAL)
    def test_put_delete_job_ok(self, scheduled_jobs, get_json_happy_dict, job_status):
        # post a new job
        job = get_json_happy_dict
        input_string = simplejson.dumps(job)
        et_scanner_queue = FakeSQS()
        result = self.post_job(scheduled_jobs, et_scanner_queue, input_string)
        required_args = list(ACTION_TO_REQUIRED_ARGS['put'])
        new_happy_dict = dict(zip(required_args, [get_json_happy_dict[k] for k in required_args]))
        new_happy_dict['et_status'] = job_status
        new_happy_dict['delete_requested'] = True

        # update job status
        hash_key = _create_hash_key(new_happy_dict)
        job = scheduled_jobs.get(hash_key=hash_key)
        # make state machine happy
        job.update(et_status=JOBS_ETL_STATUS_SCHEDULED)
        job.update(et_status=JOBS_ETL_STATUS_RUNNING)
        job.update(et_status=job_status)

        input_string = simplejson.dumps(new_happy_dict)
        result = put_job(scheduled_jobs, et_scanner_queue, input_string)
        assert 'put_accepted' in result
        assert result['put_accepted'] is True
        assert et_scanner_queue._published_msg == {'message': 'job put request'}

    def test_put_delete_job_fail(self, scheduled_jobs, get_json_happy_dict):
        # post a new job
        job = get_json_happy_dict
        input_string = simplejson.dumps(job)
        et_scanner_queue = FakeSQS()
        self.post_job(scheduled_jobs, et_scanner_queue, input_string)
        required_args = list(ACTION_TO_REQUIRED_ARGS['put'])
        new_happy_dict = dict(zip(required_args, [get_json_happy_dict[k] for k in required_args]))
        new_happy_dict['delete_requested'] = True
        input_string = simplejson.dumps(new_happy_dict)
        with pytest.raises(ValueError):
            put_job(scheduled_jobs, et_scanner_queue, input_string)

    @pytest.mark.parametrize("action",
                             ['pause', 'cancel'])
    def test_put_action_wrong_type(self, scheduled_jobs, get_json_happy_dict, action):
        # post a new job
        input_string = simplejson.dumps(get_json_happy_dict)
        et_scanner_queue = FakeSQS()
        self.post_job(scheduled_jobs, et_scanner_queue, input_string)
        # update the action of the job
        required_args = list(ACTION_TO_REQUIRED_ARGS['put'])
        new_happy_dict = dict(zip(required_args, [get_json_happy_dict[k] for k in required_args]))
        new_happy_dict[action] = '1'    # fill with wrong type. It should be boolean here.
        input_string = simplejson.dumps(new_happy_dict)
        with pytest.raises(ValueError):
            put_job(scheduled_jobs, et_scanner_queue, input_string)

    @pytest.mark.parametrize("missing_key",
                             ['redshift_id', 'log_name', 'log_schema_version', 'start_date'])
    def test_put_insufficient_key(self, missing_key, scheduled_jobs, get_json_happy_dict):
        # post a new job
        input_string = simplejson.dumps(get_json_happy_dict)
        et_scanner_queue = FakeSQS()
        self.post_job(scheduled_jobs, et_scanner_queue, input_string)
        # put job with missing args
        test_dict = dict(get_json_happy_dict)
        test_dict.pop(missing_key)
        input_string = simplejson.dumps(test_dict)
        with pytest.raises(PrimaryKeyError):
            put_job(None, None, input_string)

    def test_put_no_kwargs(self):
        with pytest.raises(JSONDecodeError):
            put_job(None, None, "")
