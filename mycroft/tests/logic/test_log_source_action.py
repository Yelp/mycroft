# -*- coding: utf-8 -*-
from contextlib import nested
import mock
import pytest

from requests import Response
from requests.exceptions import HTTPError

from mycroft.logic.log_source_action import search_log_source_by_keyword


class TestLogSourceAction(object):
    @pytest.yield_fixture(scope='function')
    def mock_response(self):
        with nested(
            mock.patch('mycroft.logic.log_source_action.staticconf.read_bool',
                       autospec=True),
            mock.patch('mycroft.logic.log_source_action.staticconf.read_string',
                       autospec=True),
            mock.patch('mycroft.logic.log_source_action.requests.post',
                       autospec=True)
        ) as (
            read_bool,
            read_string,
            mock_requests_post
        ):
            read_bool.return_value = False
            mock_response = Response()
            mock_requests_post.return_value = mock_response
            yield mock_response

    @pytest.mark.parametrize("error_code", [
        (404),
        (500),
    ])
    def test_search_log_source_with_errors(self, mock_response, error_code):
        mock_response.status_code = error_code
        request_body = "{'keyword': 'fake_request'}"

        with pytest.raises(HTTPError):
            search_log_source_by_keyword(request_body)
