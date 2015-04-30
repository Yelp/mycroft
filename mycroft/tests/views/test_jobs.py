# -*- coding: utf-8 -*-
from mock import patch
import pytest

from mycroft.models.abstract_records import PrimaryKeyError
from mycroft.views.jobs import jobs
from mycroft.views.jobs import jobs_filtered
from mycroft.views.jobs import jobs_update_job
from tests.views.conftest import dummy_request


@pytest.mark.parametrize("err_instance, expected_return_code", [
    (ValueError(), 404),
    (Exception(), 500)
])
def test_jobs_filtered_errors(err_instance, expected_return_code):
    dr = dummy_request()
    dr.matchdict["log_name"] = "dummy_log_name"
    with patch('mycroft.models.aws_connections.TableConnection.get_connection'):
        with patch('mycroft.views.jobs.list_jobs_by_name') as list_action:
            list_action.side_effect = err_instance
            actual_return_code, _ = jobs_filtered(dr)
        assert actual_return_code == expected_return_code


@pytest.mark.parametrize("err_instance, expected_return_code", [
    (PrimaryKeyError(), 400),
    (ValueError(), 404),
    (ValueError("ConditionalCheckFailedException"), 404),
    (Exception, 500),
])
def test_jobs_errors(err_instance, expected_return_code):
    dr = dummy_request()
    with patch('mycroft.models.aws_connections.TableConnection.get_connection'):
        with patch('mycroft.views.jobs.list_all_jobs') as list_all_jobs:
            list_all_jobs.side_effect = err_instance
            actual_return_code, _ = jobs(dr)
        assert actual_return_code == expected_return_code


@pytest.mark.parametrize("request_method, parm", [
    ("POST", "mycroft.views.jobs.post_job"),
    ("GET", "mycroft.views.jobs.list_all_jobs")])
def test_jobs(request_method, parm):
    dr = dummy_request()
    dr.method = request_method
    expected_return_code = 200
    with patch('mycroft.models.aws_connections.TableConnection.get_connection'):
        with patch('mycroft.views.jobs.get_scanner_queue'):
            with patch(parm) as list_action:
                list_action.return_value = {}
                actual_return_code, _ = jobs(dr)
            assert actual_return_code == expected_return_code


@pytest.mark.parametrize("log_version, parm", [
    ("dummy_logversion", "mycroft.views.jobs.list_jobs_by_name_version"),
    (None, "mycroft.views.jobs.list_jobs_by_name")])
def test_jobs_filtered(log_version, parm):
    dr = dummy_request()
    dr.matchdict["log_name"] = "dummy_log_name"
    dr.matchdict["log_schema_version"] = log_version
    expected_return_code = 200
    with patch('mycroft.models.aws_connections.TableConnection.get_connection'):
        with patch(parm) as list_action:
            list_action.return_value = {}
            actual_return_code, _ = jobs_filtered(dr)
        assert actual_return_code == expected_return_code


@pytest.mark.parametrize("err_instance, expected_return_code", [
    (PrimaryKeyError(), 400),
    (ValueError(), 404),
    (Exception, 500),
])
def test_jobs_update_job_errors(err_instance, expected_return_code):
    dr = dummy_request()
    with patch('mycroft.models.aws_connections.TableConnection.get_connection'):
        with patch('mycroft.views.jobs.get_scanner_queue'):
            with patch('mycroft.views.jobs.put_job') as put_job:
                put_job.side_effect = err_instance
                actual_return_code, _ = jobs_update_job(dr)
            assert actual_return_code == expected_return_code


@pytest.mark.parametrize("request_method, parm", [
    ("PUT", "mycroft.views.jobs.put_job")])
def test_jobs_update_job(request_method, parm):
    dr = dummy_request()
    dr.method = request_method
    expected_return_code = 200
    with patch('mycroft.models.aws_connections.TableConnection.get_connection'):
        with patch('mycroft.views.jobs.get_scanner_queue'):
            with patch(parm) as list_action:
                list_action.return_value = {}
                actual_return_code, _ = jobs_update_job(dr)
            assert actual_return_code == expected_return_code
