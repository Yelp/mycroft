# -*- coding: utf-8 -*-
import mock
import staticconf.testing
import pytest

from mycroft.models.aws_connections import get_s3_connection
from mycroft.models.aws_connections import get_sqs_connection
from mycroft.models.aws_connections import get_dynamodb_connection
from mycroft.models.aws_connections import get_boto_creds

from tests.data.mock_config import MOCK_CONFIG


@pytest.mark.parametrize("import_path,connection_fn", [
    ("boto.dynamodb2.connect_to_region", get_dynamodb_connection),
    ("boto.s3.connect_to_region", get_s3_connection),
    ("boto.sqs.connect_to_region", get_sqs_connection),
])
def test_connections(import_path, connection_fn):
    with staticconf.testing.MockConfiguration(MOCK_CONFIG):
        with mock.patch(import_path, autospec=True) as mock_boto:
            connection_fn()
            mock_boto.assert_called_once_with(
                MOCK_CONFIG['aws_config']['region'],
                **get_boto_creds()
            )
