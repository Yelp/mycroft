# -*- coding: utf-8 -*-
import mock
import pytest
from requests.exceptions import HTTPError

from mycroft.views.log_source import log_source_search
from tests.views.conftest import dummy_request


class TestViewSourceLogs(object):
    @pytest.yield_fixture(scope='function')
    def mock_search(self):
        with mock.patch('mycroft.views.log_source.search_log_source_by_keyword',
                        autospec=True) as mock_search:
            yield mock_search

    def test_valid_search(self, mock_search):
        dr = dummy_request()
        expected_return_code = 200
        mock_search.return_value = []

        actual_return_code, _ = log_source_search(dr)
        assert actual_return_code == expected_return_code

    @pytest.mark.parametrize("error_instance, expected_return", [
        (ValueError(), 404),
        (HTTPError, 404),
        (Exception, 500),
    ])
    def test_search_errors(self, mock_search, error_instance, expected_return):
        dr = dummy_request()
        mock_search.side_effect = error_instance

        actual_return_code, _ = log_source_search(dr)
        assert actual_return_code == expected_return
