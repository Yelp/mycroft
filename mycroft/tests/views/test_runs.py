# -*- coding: utf-8 -*-
from mock import patch
import pytest

from mycroft.views.runs import runs_filtered
from tests.views.conftest import dummy_request


@pytest.mark.parametrize("err_type, expected_return_code", [
    (ValueError, 404),
    (Exception, 500),
    (None, 200)
])
def test_runs_filtered(err_type, expected_return_code):
    dr = dummy_request()
    with patch('mycroft.models.aws_connections.TableConnection.get_connection'):
        with patch('mycroft.views.runs.list_runs_by_job_id') as list_runs:
            if err_type is not None:
                list_runs.side_effect = err_type()
            else:
                list_runs.returns = {}
            actual_return_code, _ = runs_filtered(dr)
        assert actual_return_code == expected_return_code
