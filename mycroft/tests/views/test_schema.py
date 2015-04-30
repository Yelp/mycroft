# -*- coding: utf-8 -*-
from mock import patch
import pytest

from boto.s3.connection import S3ResponseError

from mycroft.views.schema import schema
from mycroft.views.schema import schema_name_and_version
from tests.views.conftest import dummy_request


@pytest.mark.parametrize("err_instance, expected_return_code", [
    (ValueError(), 404),
    (Exception(), 500),
    (S3ResponseError(400, "missing"), 400),
])
def test_schema_name_and_version_errors(err_instance, expected_return_code):
    dr = dummy_request()
    with patch('mycroft.views.schema.s3_bucket_action') as bucket_action:
        bucket_action.side_effect = err_instance
        actual_return_code, _ = schema_name_and_version(dr)
    assert actual_return_code == expected_return_code


@pytest.mark.parametrize("err_instance, expected_return_code", [
    (ValueError(), 404),
    (Exception(), 500),
    (S3ResponseError(400, "missing"), 400),
])
def test_schema_errors(err_instance, expected_return_code):
    dr = dummy_request()
    with patch('mycroft.views.schema.s3_bucket_action') as bucket_action:
        bucket_action.side_effect = err_instance
        actual_return_code, _ = schema(dr)
    assert actual_return_code == expected_return_code


@pytest.mark.parametrize("request_method", ["POST", "GET"])
def test_schema_name_and_version(request_method):
    dr = dummy_request()
    dr.method = request_method
    expected_return_code = 200
    with patch('mycroft.views.schema.s3_bucket_action') as bucket_action:
        bucket_action.return_value = {}
        actual_return_code, _ = schema_name_and_version(dr)
    assert actual_return_code == expected_return_code


@pytest.mark.parametrize("log_name", ["dummy_logname", None])
def test_schema(log_name):
    dr = dummy_request()
    dr.matchdict["log_name"] = log_name
    expected_return_code = 200
    with patch('mycroft.views.schema.s3_bucket_action') as bucket_action:
        bucket_action.return_value = {}
        actual_return_code, _ = schema(dr)
    assert actual_return_code == expected_return_code
