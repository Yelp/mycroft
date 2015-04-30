# -*- coding: utf-8 -*-

import mock

from sherlock.batch.ingest_multiple_dates import ParallelEtStepper, ETLStepper
from sherlock.batch.ingest_multiple_dates import ETLStep, _executor
from sherlock.batch.ingest_multiple_dates import KeyboardInterruptError
from sherlock.batch.ingest_multiple_dates import parse_command_line
from sherlock.batch.ingest_multiple_dates import ingest_multiple_dates_main

import staticconf.testing
from tests.data.mock_config import MOCK_CONFIG

from multiprocessing import ProcessError
import pytest

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

DUMMY_CPU_COUNT = 8


@pytest.fixture(params=[
    ['-r', '-s', '2014-05-01', '-e', '2014-05-01'],
    ['-r', '-s', '2014-05-01', '-e', '2014-05-12', '--retry-errors'],
    ['-r', '-s', '2014-05-12', '-e', '2014-05-01', '-p', '5'],
    ['-s', '2014-05-01', '-e', '2014-07-12', '-p', '50'],
    ['-s', '2014-05-01', '-e', '2014-05-02', '-p', '5'],
    ['-s', '2014-05-01', '-e', '2014-05-02', '-p', '9'],
    ['-s', '2014-05-01', '-e', '2014-05-12', '-p', '9'],
    ['-s', '2014-05-01', '-e', '2014-05-12', '-p', '-7'],
    ['-s', '2014-05-01', '-e', '2014-05-12', '-p', '9',
     '--exceed-max-processes'],
    ['-r', '-s', '2014-05-01', '-e', '2014-05-01',
     '--load-polling-interval', '1'],
])
def et_steppers(request):
    args_namespace = parse_command_line(dummy_args + request.param)
    return [ParallelEtStepper(args_namespace), ETLStepper(args_namespace)]


@pytest.mark.parametrize("input_args, expected_value", [
    (['-r', '-s', '2014-05-01', '-e', '2014-05-12', '--retry-errors'], 1),
    (['-r', '-s', '2014-05-12', '-e', '2014-05-01', '-p', '5'], 5),
    (['-s', '2014-05-01', '-e', '2014-07-12', '-p', '50'], DUMMY_CPU_COUNT),
    (['-s', '2014-05-01', '-e', '2014-05-02', '-p', '5'], 2),
    (['-s', '2014-05-01', '-e', '2014-05-02', '-p', '9'], 2),
    (['-s', '2014-05-01', '-e', '2014-05-12', '-p', '9'], DUMMY_CPU_COUNT),
    (['-s', '2014-05-01', '-e', '2014-05-12', '-p', '-7'], 1),
    (['-s', '2014-05-01', '-e', '2014-05-12', '-p', '9',
     '--exceed-max-processes'], 9)
])
def test_pool_size(input_args, expected_value):
    args_namespace = parse_command_line(dummy_args + input_args)
    pes = ParallelEtStepper(args_namespace)
    pes._setup_pool_size(args_namespace, DUMMY_CPU_COUNT)
    assert pes.pool_size == expected_value


def test_construct_steps(et_steppers):
    for et_stepper in et_steppers:
        et_stepper._construct_etl_generator()
        load_steps = [step for step in et_stepper.load_generator]
        et_steps = [step for step in et_stepper.et_generator]
        span = (et_stepper.end - et_stepper.start).days + 1
        assert len(load_steps) == span
        assert len(et_steps) == span
        assert [step for step in et_steps if step.step_type != 'et'] == []
        assert [step for step in load_steps if step.step_type != 'load'] == []


class EtlTest(ETLStep):

    def __init__(self):
        super(EtlTest, self).__init__(None, 'YYYY/MM/DD', 'type')

    def execute(self):
        pass


class EtlErrorTest(EtlTest):
    def __init__(self, error_type):
        super(EtlErrorTest, self).__init__()
        self.error_type = error_type

    def execute(self):
        raise self.error_type


def validate_execution_results(results, status='success'):
    for r in results:
        assert r['status'] == status
        assert r['type'] == 'type'
        if status == 'success':
            assert r['error_info'] == {}


def test__executor():
    validate_execution_results([_executor(EtlTest())])
    validate_execution_results([_executor(EtlErrorTest(Exception))], 'error')
    with pytest.raises(KeyboardInterruptError):
        _executor(EtlErrorTest(KeyboardInterrupt))


def test_step_execution(et_steppers):
    def mock_construct(days_from_start, step_type):
        return EtlTest()

    for et_stepper in et_steppers:
        setattr(et_stepper, '_construct_step', mock_construct)
        validate_execution_results(et_stepper.execute_et_steps())
        validate_execution_results(et_stepper.execute_load_steps())


@pytest.mark.parametrize("error_type, exception_type", [
    (Exception, ProcessError),
    (KeyboardInterrupt, BaseException),
])
def test_error_handling(et_steppers, error_type, exception_type):
    def mock_construct(days_from_start, step_type):
        return EtlErrorTest(error_type)

    for et_stepper in et_steppers:
        setattr(et_stepper, '_construct_step', mock_construct)
        with pytest.raises(exception_type):
            et_stepper.execute_et_steps()
        with pytest.raises(exception_type):
            et_stepper.execute_load_steps()


@pytest.mark.parametrize("optional_args", [
    ['-r', '-s', '2014-05-01', '-e', '2014/05/02'],
    ['-r', '-s', '2014-05-01', '-e', '2014-14-02'],
])
def test_exit_on_bad_input_date(optional_args):
    with pytest.raises(SystemExit):
        parse_command_line(dummy_args + optional_args)


def _test_ingest_multiple_dates_main(params):
    with mock.patch(
        'sherlock.batch.ingest_multiple_dates.ETLStep',
        return_value=EtlTest(),
        autospec=True,
    ):
        validate_execution_results(ingest_multiple_dates_main(params))


def _test_ingest_multiple_dates_main_exceptions(params):
    excToStatus = {
        KeyboardInterruptError: (
            'cancelled' if params.load_only or params.serial_stepper else 'error'
        ),
        KeyboardInterrupt: 'cancelled',
        ProcessError: 'error',
        SystemExit: ('unknown' if params.load_only or params.serial_stepper else 'error')
    }
    for exc_type in excToStatus.keys():
        with mock.patch(
            'sherlock.batch.ingest_multiple_dates.ETLStep',
            return_value=EtlErrorTest(exc_type),
            autospec=True,
        ):
            expected_status = excToStatus[exc_type]
            validate_execution_results(
                ingest_multiple_dates_main(params), expected_status
            )


@pytest.mark.parametrize("optional_args", [
    ['-r', '-s', '2014-05-01', '-e', '2014-05-02', '--load-only'],
    ['-r', '-s', '2014-05-01', '-e', '2014-05-02', '--et-only'],
    ['-r', '-s', '2014-05-01', '-e', '2014-05-02', '--serial-stepper'],
])
def test_ingest_multiple_dates_main(optional_args):
    with staticconf.testing.MockConfiguration(MOCK_CONFIG):
        params = parse_command_line(dummy_args + optional_args)
        with mock.patch(
            'staticconf.YamlConfiguration', return_value={}, autospec=True
        ):
            _test_ingest_multiple_dates_main(params)
            _test_ingest_multiple_dates_main_exceptions(params)
