# -*- coding: utf-8 -*-
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import STRING
import pytest
import staticconf.testing

from mycroft.logic.aws_resources import check_dynamodb
from mycroft.logic.aws_resources import dynamodb_table_names
from tests.models.test_abstract_records import dynamodb_connection  # noqa
from tests.data.mock_config import MOCK_CONFIG


def test_dynamodb_table_names():
    with staticconf.testing.MockConfiguration(MOCK_CONFIG):
        assert len(dynamodb_table_names()) > 0


@pytest.yield_fixture  # noqa
def dynamodb_table_name(dynamodb_connection):  # noqa
    table_name = 'My_Test_Table'
    table = Table.create(table_name,
                         schema=[HashKey('hash', data_type=STRING)],
                         connection=dynamodb_connection)
    yield table_name
    assert table.delete()


def test_check_dynamo_db(dynamodb_connection, dynamodb_table_name):  # noqa
    result = check_dynamodb(dynamodb_connection, [dynamodb_table_name])
    assert 'dynamodb' in result


def test_check_dynamo_db_failed(dynamodb_connection):  # noqa
    result = check_dynamodb(dynamodb_connection, ['not-exist'])
    assert 'dynamodb' in result
    assert result['dynamodb'] == 'fail'
    assert 'dynamodb_exception' in result
