# -*- coding: utf-8 -*-
import copy
import os
import sys
import staticconf

from mycroft.backend.email import Mailer
from mycroft.backend.worker.base_worker import BaseMycroftWorker
from mycroft.backend.worker.base_worker import setup_config
from mycroft.backend.worker.base_worker import WorkerJob
from mycroft.backend.worker.log_format_dict import FORMAT_TO_MRJOB
from mycroft.backend.util import date_string_generator
from mycroft.backend.util import next_date_for_string
from mycroft.log_util import log

from sherlock.common.pipeline import get_base_args_parser
from sherlock.batch.ingest_multiple_dates import parse_command_line
from sherlock.batch.s3_to_psv import MAX_CORES

MAX_SECONDS_FOR_LOAD = 60000


def parse_cmd_args(sys_argv):
    """ Parse cmd args for workers
    :param sys_argv: argv from command line
    :type sys_argv: list of string

    :returns: a namespace parsed from sys argv
    :rtype: args.namespace
    """
    parser = get_base_args_parser()
    parser.add_argument(
        "--dummy-run",
        action="store_true",
        help="use this to skip etl process for test goal"
    )
    args = parser.parse_args(sys_argv[1:])
    return args


class ImdWorkerJob(WorkerJob):
    def __init__(self, worker, msg_dict, max_runs_in_flight):
        super(ImdWorkerJob, self).__init__(
            worker, msg_dict, max_runs_in_flight
        )
        self.start = self.msg_dict['script_start_date_arg']
        self.end = self.msg_dict['script_end_date_arg']
        self.step = 1
        self.et_date_gen = date_string_generator(self.start, self.end, self.step)
        self.load_date_gen = date_string_generator(self.start, self.end, self.step)
        self.scheduled_et_date = None
        self.scheduled_load_date = None
        self.load_runs_in_flight = 0
        self.stop_load_due_to_failure = False

    def run_complete(self, run_date, step, results):
        if step == 'load':
            if results[0]['status'] != 'success':
                if self.stop_load_due_to_failure:
                    raise ValueError("Unexpected 2nd failure: {0}".format(run_date))
                self.stop_load_due_to_failure = True
            else:
                if self.scheduled_load_date != run_date or \
                   not self._is_ready_for_load_step(run_date) or \
                   self.load_runs_in_flight != 1:
                    raise ValueError(
                        "Unexpected load completion: {0}, job: {1}".format(run_date, self)
                    )
                self.load_runs_in_flight -= 1

                # By default last_successful_date is written when entire job is done
                # If worker restarts, we could load the same data again.
                # This code prevents repeat loads
                kwargs = {'et_last_successful_date': run_date}
                self.worker._update_scheduled_jobs(self.msg_dict['hash_key'], kwargs)

        elif step != 'et':
            ValueError("Unexpected '{0}'".format(step))

    def _is_ready_for_load_step(self, run_date):
        run_results = self.all_results.get(run_date)
        if not run_results:
            return False

        return run_results[-1].get('status', 'unknown') == 'success'

    def _has_more_load_runs_to_schedule(self):
        if self.stop_load_due_to_failure:
            return False

        if self.load_runs_in_flight != 0:
            return False    # only single load run in flight is possible for redshift

        if self.scheduled_load_date is None:
            return self._is_ready_for_load_step(self.start)

        if self.scheduled_load_date == self.end:
            return False

        return self._is_ready_for_load_step(
            next_date_for_string(self.scheduled_load_date, self.step)
        )

    def _has_more_et_runs_to_schedule(self):
        return self.scheduled_et_date is None or self.scheduled_et_date != self.end

    def has_incomplete_runs(self):
        return self._has_more_load_runs_to_schedule()

    def has_more_runs_to_schedule(self):
        return self._has_more_et_runs_to_schedule()

    def schedule_next_run(self):
        """ Schedule next run to execute.
        First check if we can do load to make data available ASAP.
        Return a callback with name tailored to match status values
        in ETLRecords table in a backwards compatible manner
        """
        step = None

        if self._has_more_load_runs_to_schedule():
            date_string = self.scheduled_load_date = self.load_date_gen.next()
            self.load_runs_in_flight += 1
            step = 'load'
        elif self._has_more_et_runs_to_schedule():
            date_string = self.scheduled_et_date = self.et_date_gen.next()
            step = 'et'
        else:
            raise ValueError("Unexpected call to schedule next run for job: {0}".format(self))

        return WorkerJob.RunTuple(
            date_string,
            step,
            self.worker.ingest_multiple_dates,
            [self.get_args_for_step(date_string, step)],
        )

    def get_args_for_step(self, date_string, step):
        msg_copy = copy.deepcopy(self.msg_dict)
        msg_copy['script_start_date_arg'] = date_string
        msg_copy['script_end_date_arg'] = date_string
        args = self.worker._get_default_args_list()
        args.extend(self.worker._get_additional_args_for_step(msg_copy, step))
        args_obj = parse_command_line(args)
        return args_obj


class ImdWorker(BaseMycroftWorker):

    """
    Worker capable of doing ET and Load steps on each work message fetched off an SQS
    queue. See :class:`mycroft.backend.worker.base_worker.BaseMycroftWorker`
    for more details.
    """

    KEYS_TO_LOAD = [
        "redshift_region_uri",
        "log_stream_name",
        "s3_mrjob_output_path_format",
        "s3_schema_location_format",
        "default_mrjob",
    ]

    def __init__(self, config_loc, config_override_loc, run_local, emailer, dummy_run=False):
        super(ImdWorker, self).__init__(
            config_loc,
            config_override_loc,
            emailer,
            num_processes=3,
        )
        for key in self.KEYS_TO_LOAD:
            self.__setattr__(key, staticconf.read_string(key))
        if dummy_run:
            log("Dummy worker! Skip the real etl process. Just for test.")
            import mycroft.backend.worker.fake_ingest_multiple_dates as ingest_multiple_dates
        else:
            import sherlock.batch.ingest_multiple_dates as ingest_multiple_dates
        self._should_run_local = run_local
        self.dummy_run = dummy_run
        self.ingest_multiple_dates = ingest_multiple_dates.ingest_multiple_dates_main
        self.queue_name = staticconf.get_string("sqs.et_queue_name")
        self.scanner_queue_name = staticconf.get_string("sqs.et_scanner_queue_name")

        log("ImdWorker initialization")
        log(dict((k, str(v))for k, v in vars(self).iteritems()))

    def create_worker_job(self, job_request):
        return ImdWorkerJob(
            self,
            job_request,
            self._num_processes)

    def _get_default_args_list(self):
        """ Return the base list of args for sherlock that remains unchanged
        across multiple invocations of this worker's process_msg.
        """
        args = [
            # First arg, does not matter
            "ingest_multiple_dates.py",

            # We do not want to record status in redshift
            "--skip-progress-in-redshift",

            # run parallelism in the worker instead
            "--serial-stepper",
        ]

        cur_base_dir = os.getcwd()
        if self._should_run_local:
            private_file = os.path.join(
                cur_base_dir,
                staticconf.read_string("run_local.private", "private.yaml")
            )
            args.extend(["-r"])  # For run-local in sherlock
        else:
            private_file = staticconf.read_string("run_service.private")

        if self._config_override_loc is not None:
            args.extend(["--config-override", self._config_override_loc])
        args.extend(["--private", private_file])
        args.extend(["--config", self._config_loc])

        return args

    def _get_common_io_yaml_contents(self, msg_dict):
        """ Create common io yaml args for use by sherlock's tools.
        :param msg_dict: dictionary object containing parameters necessary
            to perform ET or Load steps.

        :return a dict object to be extended by step specific parameters
        """
        # Fill in values from msg_dict into path variables
        s3_output_loc = self.s3_mrjob_output_path_format.format(
            "{logname}",  # sherlock will expand this arg
            msg_dict['log_name'], msg_dict['log_schema_version']
        )
        yaml_schema_loc = self.s3_schema_location_format.format(
            msg_dict['log_name'], msg_dict['log_schema_version']
        )
        # Setup some more variables from msg_dict, falling back to defaults
        region = msg_dict.get('redshift_region', self.redshift_region_uri)
        args_dict = msg_dict['additional_arguments']
        redshift_database = args_dict.get('redshift_database', 'dev')

        return {
            'pipeline.s3_output_prefix': s3_output_loc,
            'pipeline.yaml_schema_file': yaml_schema_loc,
            'redshift_region_uri': region,
            'redshift_host': msg_dict['redshift_host'],
            'redshift_port': msg_dict['redshift_port'],
            'redshift_schema': msg_dict['redshift_schema'],
            'pipeline.redshift_database': redshift_database,
        }

    def _get_additional_args_for_step(self, message_dict, step_type):
        """ Create a list of additional arguments from the message dictionary
        :param message_dict: the message from the queue in dictionary form
        :type message_dict: dict

        :returns: a list of additional command line arguments for the step_type
        :rtype: list
        """
        step_type_key = "{0}_step".format(step_type)
        step_type_arg = "--{0}-only".format(step_type)
        io_yaml = self._get_io_yaml_arg_for_sherlock(message_dict)
        args_list = [
            "--io_yaml", str(io_yaml), step_type_arg,
            "-s", message_dict['script_start_date_arg'],
            "-e", message_dict['script_end_date_arg'],
        ]
        args_dict = message_dict.get('additional_arguments', {})
        args_list.extend(args_dict.get('common', []))
        args_list.extend(args_dict.get(step_type_key, []))
        args_list.append(io_yaml['pipeline.yaml_schema_file'])
        return args_list

    def _get_queue_name(self):
        return self.queue_name

    def _get_scanner_queue_name(self):
        return self.scanner_queue_name

    def _get_io_yaml_arg_for_sherlock(self, msg_dict):
        """ Create io yaml arg for use by sherlock's tools.
        :param msg_dict: dictionary object containing information in the ET sqs
            message. Obtained from an instance of JSONMessage object.

        :return a dictionary object to be passed to sherlock
        """
        # Worker should get the log_format from front-end through log_finder service which
        # is passed in by msg_dict. However, to make it backward-compitable, we keep the way
        # of overriding the log format through addictional arguments.
        args_dict = msg_dict['additional_arguments']
        log_format = args_dict.get('log_format', msg_dict.get('log_format') or 'json')
        cores = min(args_dict.get('pipeline.et_step.cores', 10), MAX_CORES)
        copy_time_est_secs = args_dict.get(
            'pipeline.load_step.copy_time_est_secs',
            MAX_SECONDS_FOR_LOAD
        )

        mrjob = self._find_mrjob_with_format(log_format)
        s3_input_suffix = msg_dict.get('s3_log_suffix', '*')

        # Put together yaml spec needed by sherlock
        yaml_config = self._get_common_io_yaml_contents(msg_dict)
        yaml_config.update({
            'pipeline.et_step.s3_prefixes': [
                msg_dict['s3_path']
            ],
            'pipeline.et_step.cores': cores,
            'pipeline.et_step.s3_to_s3_stream': self.log_stream_name,
            'pipeline.et_step.s3_input_suffix': s3_input_suffix,
            'pipeline.et_step.mrjob': mrjob,
            'pipeline.load_step.s3_to_redshift_stream': self.log_stream_name,
            'pipeline.load_step.days_to_check': 1,
            'pipeline.load_step.copy_time_est_secs': copy_time_est_secs,
        })
        return yaml_config

    def _find_mrjob_with_format(self, log_format):
        """ Look up the log format in the FORMAT_TO_MRJOB dictionary
            return path of the mrjob to use
        """
        # TODO: replace FORMAT_TO_MRJOB dict with a dynamo db table
        mrjob_entry = FORMAT_TO_MRJOB.get(log_format)
        if mrjob_entry is None:
            mrjob_entry = FORMAT_TO_MRJOB['custom'].format(log_format)
        return mrjob_entry


if __name__ == "__main__":
    args = parse_cmd_args(sys.argv)
    setup_config(args, 'ImdWorker')
    try:
        ImdWorker(
            args.config, args.config_override, args.run_local,
            Mailer(args.run_local), args.dummy_run
        ).run()
    except KeyboardInterrupt:
        pass
