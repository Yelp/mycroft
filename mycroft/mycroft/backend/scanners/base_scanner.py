# -*- coding: utf-8 -*-
from datetime import timedelta
from datetime import datetime
from copy import copy
import time
from simplejson import loads

import staticconf
from mycroft.logic.cluster_actions import list_cluster_by_name
from mycroft.logic.log_source_action import get_log_meta_data
from mycroft.log_util import log
from mycroft.log_util import log_exception
from mycroft.models.aws_connections import TableConnection
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_SCHEDULED
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_SUCCESS
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_PAUSED
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_RUNNING
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_EMPTY
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_ERROR
from mycroft.models.scheduled_jobs import JOBS_ETL_ACTIONS

from sherlock.common.schema import get_deep
from sherlock.common.pipeline import get_base_args_parser
from sherlock.common.util import parse_s3_path


class BaseScanner(object):

    """ A base class for Scanners for any job type (ET/Load). Scanners look for
    work items from a given DB and enqueue work items into SQS.
    """

    DEFAULT_KEYS_TO_FETCH_FROM_JOB = {
        'additional_arguments': None,
        'contact_emails': None,
        'end_date': None,
        'et_last_successful_date': None,
        'et_status': None,
        'hash_key': None,
        'load_last_successful_date': None,
        'log_format': None,
        'log_name': None,
        'log_schema_version': None,
        'redshift_id': None,
        's3_path': None,
        'start_date': None,
        'time_log_need_to_be_available': None,
        'uuid': None,
    }

    def __init__(self, db_obj, sqs_scanner_queue, sqs_worker_queue, emailer):
        """
        :param db_obj: dynamodb table of scheduled jobs
        :type db_obj: boto.dynamodb2.table.Table

        :param sqs_scanner_queue: scanner queue which to send a feed back message
        :type sqs_scanner_queue: boto.sqs.queue.Queue

        :param sqs_worker_queue: worker queue which to receive job from
        :type sqs_worker_queue: boto.sqs.queue.Queue

        """
        self.db = db_obj
        self.scanner_queue = sqs_scanner_queue
        self.worker_queue = sqs_worker_queue
        self._should_run = True
        self._run_once = False
        self.worker_keepalive_sec = staticconf.read_int('scanner.worker_keepalive_sec')
        self.max_error_retries = staticconf.read_int('max_error_retries')
        self.msg_max_retention_sec = int(
            self.scanner_queue.get_queue_attributes()['MessageRetentionPeriod']
        )
        self.msg_max_retention_sec += 3600  # give SQS enough time to delete the message
        self.emailer = emailer

        log("scanner initialization")
        log(dict((k, str(v))for k, v in vars(self).iteritems()))

    # TODO: Process each message instead of scan table every time.
    def run(self):
        timeout = self._get_timeout()
        last_run_time = self._get_utc_now() - timedelta(minutes=timeout)
        while(self._should_run):
            msgs = []
            try:
                msgs = self.scanner_queue.get_messages_from_queue()
            except Exception:
                # if sqs queue fails, throttle retry
                log_exception(
                    "Exception in fetching messages from queue: "
                    + str(self.scanner_queue.get_queue_name())
                )
                time.sleep(self.scanner_queue.get_wait_time())

            try:
                if (len(msgs) > 0
                        or self._get_utc_now() - last_run_time >= timedelta(minutes=timeout)):
                    if len(msgs) > 0:
                        self.scanner_queue.delete_message_batch_from_queue(msgs)
                    self.run_maint()
                    self.run_scanner()
                    self.scanner_queue.clear()
                    last_run_time = self._get_utc_now()
            except Exception:
                log_exception("Exception in running scanner")

            if self._run_once is True:
                break

    def run_scanner(self):
        """ Fetch relevant jobs from DB table and create ET or L work.
        Does not throw any exceptions out.
        """
        jobs = self._fetch_jobs_for_work()
        for job in jobs:
            try:
                if self._action_pending(job) or self._should_process_job(job):
                    self._create_work_for_job(job)
                else:
                    log("Skipping job since there was no processing needed: {0}".format(
                        job2log(job)
                    ))
            except Exception:
                log_exception(
                    "Caught an exception in processing a job. Ignoring"
                    " entry: {0}".format(job2log(job)))

    def stop(self):
        self._should_run = False

    def _fetch_jobs_for_work(self):
        """ Fetch jobs from table that qualify for a given work type.
        Implement this method in child classes for ET or Load work.

        :returns: A list of scheduled_job objects
        :rtype: list
        """
        raise NotImplementedError

    def _action_pending(self, job):
        """ Check if job has pending action

        :param job: an entry from DynamoDB of type scheduled job
        :type job: scheduled_job

        :returns: boolean signalling whether this job should be processed
        :rtype: bool
        """
        kwargs = dict((action, None) for action in JOBS_ETL_ACTIONS)

        result = job.get(**kwargs)
        for key, value in result.iteritems():
            if value not in [0, None]:
                return True
        return False

    def _should_process_job(self, job):
        """ Check if the given job needs a work item for et/l.
        This method implements some basic conditions, customize in subclasses.

        :param job: an entry from DynamoDB of type scheduled job
        :type job: scheduled_job

        :returns: boolean signalling whether this job should be processed
        :rtype: bool
        """
        start_date = job.get(start_date=None)['start_date']
        return self._data_available_for_date(job, start_date)

    def _update_job_status(self, job, status_value):
        kwargs = {"et_status": status_value}
        if job.update(**kwargs) is not True:
            log("Could not update {0} for job {1}".format(kwargs, job2log(job)))

    def _maint_scheduled_jobs(self, now):
        status_ts_field = 'et_status_last_updated_at'
        get_jobs = getattr(self.db, 'get_jobs_with_et_status')

        jobs = get_jobs(JOBS_ETL_STATUS_SCHEDULED)
        for job in jobs:
            kwargs = {status_ts_field: None}
            status_ts_value = job.get(**kwargs).get(status_ts_field, False) or str(now)

            last_update = datetime.strptime(status_ts_value, '%Y-%m-%d %H:%M:%S.%f')
            if now - last_update > timedelta(seconds=self.msg_max_retention_sec):
                log('found stuck scheduled job {0}, resetting...'.format(job2log(job)))
                self._update_job_status(job, JOBS_ETL_STATUS_EMPTY)

    def _maint_paused_jobs(self, now):
        lsd_field = 'et_last_successful_date'
        kwargs = dict((action, None) for action in JOBS_ETL_ACTIONS)
        kwargs[lsd_field] = None

        get_jobs = getattr(self.db, 'get_jobs_with_et_status')
        jobs = get_jobs(JOBS_ETL_STATUS_PAUSED)
        for job in jobs:
            # if key is set to None or does not exist, assume it's set to 1
            # to avoid resuming non-conforming jobs.
            result_dict = job.get(**kwargs)
            pause_requested = result_dict.get('pause_requested', 1)
            cancel_requested = result_dict.get('cancel_requested', 1)
            lsd = result_dict.get(lsd_field, None)

            if cancel_requested != 0:
                log('cancel requested for paused job {0}, resetting...'.format(job2log(job)))
                self._update_job_status(job, JOBS_ETL_STATUS_EMPTY)
            elif pause_requested == 0:
                # job is paused but pause_requested is 0 => resume to IDLE,
                # or SUCCESS if can't resume right away
                ready_to_process = self._should_process_job(job)
                new_job_status = JOBS_ETL_STATUS_EMPTY \
                    if ready_to_process or lsd is None else JOBS_ETL_STATUS_SUCCESS
                log('resetting paused job {0} to {1}'.format(job2log(job), new_job_status))
                self._update_job_status(job, new_job_status)
                if ready_to_process:
                    additional_info = "Job will start immediately."
                else:
                    additional_info = "Job will start when new data is available."
                job_dict = job.get(**self.DEFAULT_KEYS_TO_FETCH_FROM_JOB)
                if job_dict['contact_emails'] is not None:
                    job_dict['contact_emails'] = list(job_dict['contact_emails'])
                try:
                    self.emailer.mail_result(
                        "resumed", job_dict, additional_info
                    )
                    log("Sent emails to: {0}".format(job_dict['contact_emails']))
                except Exception:
                    log_exception("Exception in sending emails of job:" +
                                  str(job_dict))

    def _maint_running_jobs(self, now):
        status_ts_field = 'et_status_last_updated_at'
        get_jobs = getattr(self.db, 'get_jobs_with_et_status')
        jobs = get_jobs(JOBS_ETL_STATUS_RUNNING)
        for job in jobs:
            kwargs = {status_ts_field: None}
            status_ts_value = job.get(**kwargs).get(status_ts_field, False) or str(now)

            last_update = datetime.strptime(status_ts_value, '%Y-%m-%d %H:%M:%S.%f')
            if now - last_update > timedelta(seconds=self.worker_keepalive_sec):
                log('found stuck running job {0}, resetting...'.format(job2log(job)))
                self._update_job_status(job, JOBS_ETL_STATUS_EMPTY)

    def _maint_error_jobs(self, now):
        retry_ts_field = 'et_next_error_retry_attempt'
        num_retries_field = 'et_num_error_retries'
        get_jobs = getattr(self.db, 'get_jobs_with_et_status')
        jobs = get_jobs(JOBS_ETL_STATUS_ERROR)
        for job in jobs:
            kwargs = {retry_ts_field: None, num_retries_field: None}
            fields = job.get(**kwargs)

            retry_ts_value = fields.get(retry_ts_field, False) or str(now)
            num_retries = fields.get(num_retries_field, None)

            # for legacy error jobs, set to max_error_retries to avoid auto-start
            num_retries = int(num_retries) if num_retries is not None else self.max_error_retries

            next_retry = datetime.strptime(retry_ts_value, '%Y-%m-%d %H:%M:%S.%f')
            if next_retry <= now and num_retries < self.max_error_retries:
                log('attempt {0}/{1} to re-run job {2}'.format(
                    num_retries + 1, self.max_error_retries, job2log(job)
                ))
                self._update_job_status(job, JOBS_ETL_STATUS_EMPTY)

            if self._action_pending(job):
                # push job to worker for actions like cancel/pause/delete
                self._update_job_status(job, JOBS_ETL_STATUS_EMPTY)

    def run_maint(self):
        now = self._get_utc_now()
        self._maint_scheduled_jobs(now)
        self._maint_paused_jobs(now)
        self._maint_running_jobs(now)
        self._maint_error_jobs(now)

    def _create_work_for_job(self, job):
        """ Create and enqueue an sqs message for the given job.

        :param job: an entry from DynamoDB of type schedule job
        :type job: ScheduledJob
        """
        raise NotImplementedError

    def _get_date_string(self, num_days_delta):
        """ Get a YYYY-MM-DD representation of the day num_days_delta.

        :param num_days_delta: could be any integer including 0 (zero).
        :type num_days_delta: int

        :returns: a string represenation of num_days_delta days from today
        :rtype: string
        """
        today = datetime.today()
        day = today + timedelta(days=num_days_delta)
        return day.strftime("%Y-%m-%d")

    def _earlier_date(self, date1, date2):
        """ Returns the earlier (by chronological order) of two given dates.
        If either date is None, returns the other.
        If both are None, returns None.

        :param date1: a string representation of a date
        :type date1: string

        :param date2: a string representation of a date
        :type date2: string

        :returns: earlier date by chronology of the two arguments
        :rtype: string
        """
        if date1 is None or len(date1) == 0:
            return date2
        if date2 is None or len(date2) == 0:
            return date1

        return min(date1, date2)

    def _later_date(self, date1, date2):
        """ Returns the later (by chronological order) of two given dates.
        If either date is None, returns the other.
        If both are None, returns None.

        :param date1: a string representation of a date
        :type date1: string

        :param date2: a string representation of a date
        :type date2: string

        :returns: earlier date by chronology of the two arguments
        :rtype: string
        """
        if date1 is None or len(date1) == 0:
            return date2
        if date2 is None or len(date2) == 0:
            return date1

        return max(date1, date2)

    def _data_available_for_date(self, job, date_str):
        """ Returns bool for whether given date_str's data is supposed to be
        available on S3, based on the entry's data available time of day param.
        If entry does not have that parameter, assumes data is available right
        away.

        :param job: instance of ScheduledJob
        :type job: ScheduledJob

        :param date_str: string representation of a date (YYYY-MM-DD format)
        :type date_str: string

        :returns: boolean, indicating whether data is available for given date
        :rtype: bool
        """
        return date_str <= self._get_max_available_date(job)

    def _get_time_log_need_to_be_available(self, job):
        """ Return the time_log_need_to_be_avilable if user didn't set it; otherwise
        return the user custom value.

        :param job: instance of ScheduledJob
        :type job: ScheduledJob
        """
        log_avail_time_dict = job.get(time_log_need_to_be_available=None)
        log_avail_time = log_avail_time_dict.get('time_log_need_to_be_available')
        custom_avail_time_str = job.get(additional_arguments='{}')['additional_arguments']
        custom_avail_time = loads(custom_avail_time_str).get('time_log_need_to_be_available')

        return log_avail_time if custom_avail_time is None else custom_avail_time

    def _get_max_complete_date(self, job):
        """ Return the max_complete_date from aws

        :param job: instance of ScheduledJob
        :type job: ScheduledJob
        """
        job_dict = job.get(s3_path=None)
        # s3_path sample: s3://bucket_name/logs/log_name/
        s3_path = job_dict.get('s3_path')
        if s3_path is None:
            return None
        bucket_name, prefix = parse_s3_path(s3_path)
        prefix_list = prefix.split("/")
        if prefix_list[-1] is not '':
            log_name = prefix_list[-1]
        else:
            log_name = prefix_list[-2]
        try:
            log_data = get_log_meta_data(bucket_name, log_name)
            return get_deep(log_data, ['log', 'max_complete_date'], None)
        except Exception:
            log_exception(
                "Exception in running scanner when getting max_complete_date in s3 path: "
                + s3_path
            )
        return None

    def _get_max_available_date(self, job):
        """Return the max date which should be ready based the current datetime and
        time_log_need_to_be_available of the job

        :param job: instance of ScheduledJob
        :type job: ScheduledJob

        :returns: the string of the max date whose log should be available
        :rtype: string
        """
        max_complete_date = self._get_max_complete_date(job)
        if max_complete_date is not None:
            return max_complete_date

        utcnow = self._get_utc_now()
        time_log_need_to_be_available = self._get_time_log_need_to_be_available(job)
        if time_log_need_to_be_available is None:
            time_log_need_to_be_available = "48:00"
        hh, mm = time_log_need_to_be_available.split(':')
        max_available_date = utcnow - timedelta(hours=int(hh), minutes=int(mm))
        return max_available_date.strftime('%Y-%m-%d')

    def _get_utc_now(self):
        """ Return datetime.utcnow(), used for mocking away in tests.
        :returns: current time as a datetime object
        :rtype: datetime
        """
        return datetime.utcnow()

    def _get_redshift_cluster_details(self, rs_id):
        """ Get the host and port for a particular redshift id
        :param rs_id: redshift id of a cluster e.g., cluster-1
        :type rs_id: string
        :returns: a 2 tuple containing a redshift host name and redshift port
        :rtype: tuple
        """
        cluster = list_cluster_by_name(
            TableConnection.get_connection('RedshiftClusters'),
            rs_id)

        if cluster:
            port = cluster['port']
            host = cluster['host']
            schema = cluster['db_schema']
        else:
            raise ValueError("No cluster named: {0}".format(rs_id))

        return host, port, schema

    def _enqueue_work_in_sqs(
            self, new_start_date, new_end_date, job_type, job_dict):
        """ Publish a message onto an SQS queue for work on the given job_dict.
        :param new_start_date: start date for doing et/l work,
            in YYYY-MM-DD format
        :type new_start_date: string

        :param new_end_date: end date for doing et/l work, in YYYY-MM-DD format
        :type new_end_date: string

        :param job_type: a string for the type of work to do - ET or L
        :type job_type: string

        :param job_dict: dictionary from the ScheduledJob object representing
            this job
        :type job_dict: dict
        """
        # Add the start/end dates for ingest multiple script
        job_dict = copy(job_dict)
        job_dict['script_start_date_arg'] = new_start_date
        job_dict['script_end_date_arg'] = new_end_date
        job_dict['job_type'] = job_type
        job_dict['additional_arguments'] = loads(job_dict.get('additional_arguments', "{}"))

        redshift_id = job_dict['redshift_id']
        host, port, schema = self._get_redshift_cluster_details(redshift_id)
        job_dict['redshift_host'] = host
        job_dict['redshift_port'] = port
        job_dict['redshift_schema'] = schema
        if job_dict['contact_emails'] is not None:
            # set is not JSON serializable so convert this member to a list
            # should be fine since we are not going to edit this in the backend
            job_dict['contact_emails'] = list(job_dict['contact_emails'])
        self.worker_queue.write_message_to_queue(job_dict)

    def _get_timeout(self):
        """ Return the timeout of the scanner
        """
        raise NotImplementedError


def job2log(job):
    return job._record._item._data['uuid']


def parse_cmd_args(sys_argv):
    """ Parse cmd args for scanners
    :param sys_argv: argv from command line
    :type sys_argv: list of string

    :returns: a namespace parsed from sys argv
    :rtype: args.namespace
    """
    parser = get_base_args_parser()
    args = parser.parse_args(sys_argv[1:])
    return args
