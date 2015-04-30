# -*- coding: utf-8 -*-
import pytest
import staticconf.testing

from mock import mock_open, patch

from boto.exception import S3ResponseError

from sherlock.common.util import load_from_file
from sherlock.common.util import load_from_s3_file
from sherlock.common.util import get_deep

from tests.data.mock_config import MOCK_CONFIG


def test_load_from_file_with_local_file():
    file_path = './foo'
    file_content = 'stuff'

    with patch('__builtin__.open',
               mock_open(read_data=file_content),
               create=True) as m:
        result = load_from_file(file_path)
    m.assert_called_once_with(file_path, 'r')
    m().read.assert_called_once_with()
    assert result == file_content


@pytest.mark.parametrize("x, path, expected_value", [
    ({'a': 1, 'b': {'c': 5}}, 'a', 1),
    ({'a': 1, 'b': {'c': 5}}, 'b', {'c': 5}),
    ({'a': 1, 'b': {'c': 5}}, 'b.c', 5),
    ({'a': 1, 'b': {'c': 5}}, 'd', None),
    ({'a': 1, 'b': {'c': 5}}, [], {'a': 1, 'b': {'c': 5}}),
    ({'a': 1, 'b': {'c': 5}}, None, {'a': 1, 'b': {'c': 5}}),
    ({'a': 1, 'b': {'c': 5}}, [[1, 2], 3], None),
    (None, 'a.b', None),
    ({}, 'a.b', None),
])
def test_get_deep(x, path, expected_value):
    v = get_deep(x, path)
    assert v == expected_value


def test_load_from_file_with_no_local_file():
    file_path = 'http://foo'
    expected_exception_args = (2, 'No such file or directory')
    try:
        load_from_file(file_path)
        assert 0
    except Exception as e:
        assert e.args == expected_exception_args


def test_load_from_file_with_s3_file():
    file_path = 's3://blah-foo-us-west-2/foo'
    file_content = 'stuff'

    with patch('sherlock.common.util.load_from_s3_file',
               autospec=True) as mock_load:
        mock_load.return_value = file_content
        result = load_from_file(file_path)
        assert result == file_content


class S3FakeRegion(object):
    def __init__(self, name, status=200):
        self.name = name
        self.status = status


S3_REGIONS = [
    S3FakeRegion('us-gov-west-1', 403),
    S3FakeRegion('eu-east-1', 301),
    S3FakeRegion('us-west-2'),
]


@pytest.mark.parametrize("region, s3_uri, expected_content", [
    (S3_REGIONS[0], 's3://blah-logs-us-west-2/logs/log', 'stuff'),
    (S3_REGIONS[1], 's3://blah-logs-no-region/logs/log', 'stuff'),
    (S3_REGIONS[2], 's3://blah-logs-no-region/logs/log', 'stuff'),
])
def test_load_from_s3_file(region, s3_uri, expected_content):
    def side_effect_func(*args, **kwargs):
        region_name = args[0]
        for r in S3_REGIONS:
            if r.name == region_name:
                if r.status != 200:
                    raise S3ResponseError(r.status, r.status)
                return region_name
        raise ValueError('Unknown region: {0}'.format(region_name))

    with patch('boto.s3.regions', autospec=True) as mock_regions:
        mock_regions.return_value = S3_REGIONS
        with staticconf.testing.MockConfiguration(MOCK_CONFIG):
            with patch('boto.s3.connect_to_region', autospec=True) as mock_connect:
                mock_connect.side_effect = side_effect_func
                with patch('sherlock.common.util._load_from_s3_region',
                           autospec=True) as mock_load:
                    mock_load.return_value = expected_content
                    result = load_from_s3_file(s3_uri)
                    mock_regions.assert_called_once_with()
                    assert mock_connect.call_count <= len(S3_REGIONS)
                    assert mock_load.call_count == 1
                    assert (mock_load.call_args)[0][0] == 'us-west-2'
                    assert result == expected_content
