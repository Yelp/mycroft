# -*- coding: utf-8 -*-
from mock import Mock, patch
import pytest

from mycroft.views.healthcheck import healthcheck
from mycroft.views.healthcheck import _NAME_TO_FUNC
from tests.views.conftest import dummy_request

fake_conn = Mock()
mock_get_dynamodb_conn = Mock()
mock_get_s3_conn = Mock()
mock_get_sqs_conn = Mock()


@pytest.mark.parametrize("conn,result", [
    (fake_conn, 200),
    (None, 500),
])
def test_connections(conn, result):
    dr = dummy_request()
    with patch.dict(
        _NAME_TO_FUNC,
        {
            'dynamodb': mock_get_dynamodb_conn,
            's3': mock_get_s3_conn,
            'sqs': mock_get_sqs_conn,
        }
    ):
        mock_get_s3_conn.return_value = conn
        mock_get_sqs_conn.return_value = conn
        mock_get_dynamodb_conn.return_value = conn
        res = healthcheck(dr)
        assert result in res
