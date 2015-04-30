# -*- coding: utf-8 -*-
import pytest
import mock
from mycroft.backend.worker.fake_ingest_multiple_dates import ingest_multiple_dates_main
from mycroft.backend.worker.fake_ingest_multiple_dates import get_error_info

from sherlock.batch.ingest_multiple_dates import parse_command_line


dummy_args = [
    'ingest_multiple_dates.py',
    '--io_yaml',
    'pipeline_io.yaml',
    '--config',
    'config.yaml',
    '--private',
    'user_session.yaml',
    '--config-override',
    'co.yaml',
    'db.yaml'
]


@pytest.mark.parametrize("status", [
    'success',
    'error'
])
def test_get_error_info(status):
    error_info = get_error_info(status)
    if status == 'success':
        assert error_info == {}
    elif status == 'error':
        assert len(error_info) == 2
        assert error_info['crash_tb'] == 'fake error'
        assert error_info['crash_exc'] == 'fake error'


@pytest.mark.parametrize("optional_args", [
    ['-r', '-s', '2014-05-01', '-e', '2014-05-02', '--load-only'],
    ['-r', '-s', '2014-05-01', '-e', '2014-05-02', '--et-only'],
])
def test_ingest_multiple_dates_main(optional_args):
    params = parse_command_line(dummy_args + optional_args)
    with mock.patch(
            'time.sleep', return_value=None):
        results = ingest_multiple_dates_main(params)
        assert len(results) == 2
