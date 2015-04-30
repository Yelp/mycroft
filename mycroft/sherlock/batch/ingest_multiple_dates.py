# -*- coding: utf-8 -*-
import argparse
import copy
from datetime import datetime
from datetime import timedelta
import itertools
from multiprocessing import cpu_count
from multiprocessing import Pool
from multiprocessing import ProcessError
import staticconf as sc
import sys
import time
import traceback

from sherlock.common.config_util import load_package_config
from sherlock.batch.s3_to_psv import s3_to_psv_main
from sherlock.batch.s3_to_redshift import s3_to_redshift_main
from sherlock.common.pipeline import add_load_args
from sherlock.common.pipeline import get_base_parser
from sherlock.common.pipeline import load_io_yaml_from_args


class KeyboardInterruptError(Exception):
    """
    This is required as an element of a partial solution to a bug in python
    (issues 8296 & 9205), where only exceptions inheriting from Exception are
    handled normally in a multiprocessing pool.  KeyboardInterrupt inherits
    from BaseException.
    """
    pass


class DateAction(argparse.Action):
    """
    DateAction enables us to create an action usable when we parse_args
    to convert a date from YYYY-MM-DD to a datetime, catching malformed
    and wrong dates early in our program.

    It requires only one function (aside from the constructor) which
    overrides __call__, return the datetime from the input arg
    """
    def __init__(self, *args, **kwargs):
        argparse.Action.__init__(self, *args, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        try:
            setattr(namespace,
                    self.dest, datetime.strptime(values, "%Y-%m-%d"))
        except ValueError as value_error:
            raise argparse.ArgumentError(self, value_error.args[0])


class ETLStep(object):
    """
    the ETLStep class encapsulates a step in the ETL process, now used for
    search_session to psv and then psv to redshift

    currently supports two commands:
        s3_to_psv; and
        s3_to_redshift

    Constructor Args:
    input_args -- a dictionary from an argparse namespace
    """

    step_type_to_command = {
        'et': s3_to_psv_main,
        'load': s3_to_redshift_main
    }

    def __init__(self, input_args, step_date, step_type):
        self.input_args = input_args
        self.step_date = step_date
        self.step_type = step_type
        self._add_date_to_input_args()

    def __repr__(self):
        return "{0}(type: {1} date: {2})".format(
            self.__class__.__name__,
            self.step_type,
            self.step_date
        )

    def _add_date_to_input_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--date')
        namespace_copy = copy.deepcopy(self.input_args)
        self.input_args = parser.parse_args(
            args=['--date', self.step_date],
            namespace=namespace_copy
        )

    def execute(self):
        """
        executes this step using the function defined in the
        step_type_to_command dictionary.
        """
        print self
        if not self.input_args.dry_run:
            self.step_type_to_command[self.step_type](self.input_args)


class ETLStepper(object):
    """
    A class to step through a number of dates where each step is an ETLStep.

    Arguments:
        input_args -- a ArgParse.Namespace with names / values:
            run_local -- boolean
            io_yaml -- a filepath to something like 'pipeline_io.yaml'
            conig -- a filepath to config.yaml
            config-override -- a filepath to a config-override file
            start_date -- a datetime start date
            end_date -- a datetime end date
            private -- file path to a redshift un / pw yaml file
            db_file -- file path to a yaml schema file
    """

    def __init__(self, input_args):
        self.start = input_args.start_date
        self.end = input_args.end_date
        self.poll_interval_seconds = input_args.load_polling_interval
        if self.poll_interval_seconds < 1:
            self._try_load_step = self._load_step
        else:
            self._try_load_step = self._polling_load_step
        self.input_args = input_args
        self._check_dates()
        self.et_generator = None
        self.load_generator = None

    def _check_dates(self):
        """
        ensure the start and end date are in the right order
        """
        if self.start > self.end:
            temp = self.start
            self.start = self.end
            self.end = temp

    def _construct_step(self, days_from_start, step_type):
        """
        constructs an ETLStep given the input args for that step

        Args:
        days_from_start -- an integer number of days from self.start
        step_type -- either 'load' or 'et'

        Returns:
        an ETLStep instance
        """
        step_date = (self.start +
                     timedelta(days=days_from_start)).strftime("%Y-%m-%d")
        return ETLStep(self.input_args, step_date, step_type)

    def _get_step_type_imap(self, step_type):
        """
        constructs an imap of steps from an xrange of dates from the
        start of the date range specified in the input arguments

        Args:
        step_type -- a string (either 'et' or 'load')
        """
        return itertools.imap(
            lambda days_from_start: self._construct_step(
                days_from_start, step_type
            ),
            xrange((self.end - self.start).days + 1)
        )

    def _construct_etl_generator(self, generator_type=None):
        """
        constructs imaps of et steps and load steps from date range given in
        the command line input args namespace

        Args:
        generator_type = 'et' or 'load'

        Updates:
        self.et_generator and/or self.load_generator depending on
        the generator_type specified
        """
        if generator_type == 'et':
            self.et_generator = self._get_step_type_imap('et')
        elif generator_type == 'load':
            self.load_generator = self._get_step_type_imap('load')
        else:
            self.et_generator = self._get_step_type_imap('et')
            self.load_generator = self._get_step_type_imap('load')

    def execute_et_steps(self):
        """
        executes et steps serially
        """
        self._construct_etl_generator('et')
        results = []
        for step in self.et_generator:
            step_result = _init_step_result(step)
            try:
                step.execute()
                step_result['status'] = 'success'
            except (KeyboardInterrupt, KeyboardInterruptError):
                raise
            except Exception:
                step_result['status'] = 'error'
                step_result['error_info'] = _capture_error_info()
                raise ProcessError(
                    {'results': results, 'failures': [step_result]}
                )
            finally:
                step_result['end_time'] = time.time()
                results.append(step_result)
        return results

    def execute_load_steps(self):
        """
        iterates over the load steps in the generator, using the method
        defined in the constructor based on the polling interval defined
        (or not) on the command line
        """
        self._construct_etl_generator('load')
        results = []
        for step in self.load_generator:
            step_result = _init_step_result(step)
            try:
                self._try_load_step(step)
                step_result['status'] = 'success'
            except (KeyboardInterrupt, KeyboardInterruptError):
                raise
            except Exception:
                step_result['status'] = 'error'
                step_result['error_info'] = _capture_error_info()
                raise ProcessError(
                    {'results': results, 'failures': [step_result]}
                )
            finally:
                step_result['end_time'] = time.time()
                results.append(step_result)
        return results

    def _load_step(self, step):
        """
        simply executes the load step
        """
        step.execute()

    def _polling_load_step(self, step):
        """
        polls a load step until a non-io exception.  An IOError is thrown when
        s3_to_redshift can't find any input data.  Other errors (including
        KeyboardInterrupt) will break out of the "while True:" loop.
        """
        while True:
            try:
                step.execute()
                break
            except IOError as io_error:
                print repr(io_error)
                print "sleeping {0} minutes".format(
                    self.poll_interval_seconds / 60
                )
                time.sleep(self.poll_interval_seconds)


def _capture_error_info():
    """
    Capture exception context into a dict.
    Valid only if called from 'except' clause.
    """
    exc_type, exc_value, exc_tb = sys.exc_info()
    return {
        'crash_tb': ''.join(traceback.format_tb(exc_tb)),
        'crash_exc': traceback.format_exception_only(
            exc_type, exc_value
        )[0].strip(),
    }


def _init_step_result(step_object):
    """
    Initializer for step_result structure.
    """
    return {
        'status': 'unknown',
        'date': step_object.step_date,
        'start_time': time.time(),
        'end_time': None,
        'type': step_object.step_type,
        'error_info': {},
        'repr': repr(step_object)
    }


def _executor(step_object):
    """
    Simply runs the execute method of the input step_object.

    This is split out into its own function because the multiprocessing
    library needs to pickle the function used in a map; and since there's
    a lot of overhead making an instance method pickle'able it's much
    easier to just split this out.

    Args:
    step_object -- an instance of ETLStep
    """
    step_result = _init_step_result(step_object)
    try:
        step_object.execute()
        step_result['status'] = 'success'
    except KeyboardInterrupt:
        raise KeyboardInterruptError()
    except:
        step_result['status'] = 'error'
        step_result['error_info'] = _capture_error_info()
        pass
    finally:
        step_result['end_time'] = time.time()
    return step_result


class ParallelEtStepper(ETLStepper):
    """
    This subclass of ETLStepper uses the python multiprocessing
    library to run self.pool_size processes simultaneously.
    """

    def __init__(self, input_args):
        super(ParallelEtStepper, self).__init__(input_args)
        cpus = cpu_count()
        self.pool_size = None
        self._setup_pool_size(input_args, cpus)

    def _setup_pool_size(self, cl_args, cpus):
        """
        setup_pool_size uses the pool_size and exceed_max_processes input from
        the command line, the number of cpus in the input argument, and the
        date span found from the constructor to determine a pool size.

        This is broken out so it can be nicely unit tested.

        Args:
        cl_args -- an Argparse.namespace from the command line arguments
        cpus -- an integer number of cpus

        Sets:
        self.pool_size -- the number of processes used in parallelization of
        the ET step
        """
        if cl_args.pool_size < 1:
            self.pool_size = 1
        elif cl_args.pool_size > cpus and not cl_args.exceed_max_processes:
            self.pool_size = cpus
        else:
            self.pool_size = cl_args.pool_size
        date_span = (self.end - self.start).days + 1
        self.pool_size = min(self.pool_size, date_span)

    def execute_et_steps(self):
        """
        executes et steps in parallel using a pool of size
        input_args.pool_size which is defined on the command line
        """
        self._construct_etl_generator('et')

        # use chunksize=1 for the pool because we have a small number of
        # lengthy jobs rather than a large number of short jobs
        pool = Pool(processes=self.pool_size)
        pool_results = []
        try:
            pool_results = pool.map(
                _executor,
                self.et_generator,
                chunksize=1
            )
            pool.close()
            failures = [step_result for step_result in pool_results
                        if step_result['status'] != "success"]
            if failures:
                raise ProcessError(
                    {'results': pool_results, 'failures': failures}
                )
        except KeyboardInterrupt:
            print "...keyboard interrupt in map, terminating"
            raise
        finally:
            pool.terminate()
            pool.join()
        return pool_results


def parse_command_line(sys_argv):
    """
    parse_command_line takes in sys.argv and parses the arguments other than
    the first (since it's the program name).  It starts with a parser from
    get_default_parser having the following arguments:
        -r, --run-local
        --io_yaml
        --conig
        --config-override
        --private

    and adds the following to the arguments:
        -s, --start_date
        -e, --end_date
        -p, --private
        db_file

    Returns:
    an ArgParse.Namespace
    """
    parser = get_base_parser()
    parser = add_load_args(parser)
    parser.description = """
        This program takes arguments that are the union of arguments for
        s3_to_psv and s3_to_redshift.  It processes each date using the yaml
        schema file specified in the pipeline_io.yaml file as
        'pipeline.yaml_schema_file'.

        The resulting tables will be found in the 'dev' database in the
        redshift cluster defined in the config override file in the
        'redshift_host' variable."""
    parser.add_argument(
        '-s', '--start_date',
        help='YYYY-MM-DD',
        required=True,
        action=DateAction
    )
    parser.add_argument(
        '-e', '--end_date',
        help='YYYY-MM-DD',
        required=True,
        action=DateAction
    )
    parser.add_argument(
        "-p", "--pool-size",
        type=int,
        help="processes to run simultaneously an int in [1,num cpu's]",
        required=False,
        default=1,
    )
    parser.add_argument(
        "--exceed-max-processes",
        action="store_true",
        help="use this to run more processes than cpu's",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="use this to print out the steps we will take"
    )
    stepper_type = parser.add_mutually_exclusive_group(required=False)
    stepper_type.add_argument(
        "--serial-stepper",
        action="store_true",
        help="use the serial stepper (alternates between et & load)"
    )
    stepper_type.add_argument(
        "--load-polling-interval",
        type=int,
        default=0,
        help="interval in seconds between attempts to find data to load, \
default is 0 (meaning no polling)"
    )
    etl_type = parser.add_mutually_exclusive_group(required=False)
    etl_type.add_argument(
        "--load-only",
        action="store_true",
        help="use this to do only a load step",
    )
    etl_type.add_argument(
        "--et-only",
        action="store_true",
        help="use this to do only an et step",
    )
    return parser.parse_args(sys_argv[1:])


def ingest_multiple_dates_main(args_namespace):
    """ Main entry point for this script. Will parse args passed in and run
    ET/L as appropriate.
    Does not throw any exception.
    """
    results = []
    start_time = time.time()
    try:
        load_io_yaml_from_args(args_namespace.io_yaml)
        sc.YamlConfiguration(args_namespace.config_override, optional=True)
        if args_namespace.serial_stepper:
            etl_stepper = ETLStepper(args_namespace)
        else:
            etl_stepper = ParallelEtStepper(args_namespace)

        if not args_namespace.load_only:
            try:
                results += etl_stepper.execute_et_steps()
            except ProcessError as pe:
                results += pe.args[0]['results']
                pass
        if not args_namespace.et_only:
            try:
                results += etl_stepper.execute_load_steps()
            except ProcessError as pe:
                results += pe.args[0]['results']
                pass
    except (KeyboardInterrupt, KeyboardInterruptError, BaseException) as e:
        step_result = _init_step_result(
            ETLStep(args_namespace,
                    args_namespace.start_date.strftime("%Y-%m-%d"),
                    None)
        )
        if type(e) in [KeyboardInterrupt, KeyboardInterruptError]:
            step_result['status'] = 'cancelled'
        else:
            step_result['error_info'] = _capture_error_info()
        step_result['start_time'] = start_time
        step_result['end_time'] = time.time()
        results.append(step_result)

    return results


if __name__ == '__main__':
    args_namespace = parse_command_line(sys.argv)
    load_package_config(args_namespace.config)
    results = ingest_multiple_dates_main(args_namespace)
    failures = [step_result for step_result in results
                if step_result['status'] != "success"]
    if not failures:
        print "all et steps succeeded"
    else:
        print "job FAILED, quitting"
        print "the following steps failed:"
        for f in failures:
            print f['repr'], f['error_info']
        raise ProcessError(failures)
