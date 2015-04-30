# -*- coding: utf-8 -*-
from collections import namedtuple, defaultdict
from datetime import datetime
from datetime import timedelta
import os
from multiprocessing.pool import Pool
from multiprocessing.pool import worker as default_worker
from multiprocessing.process import Process
import signal
import time
import threading

from boto.sqs.jsonmessage import JSONMessage
import staticconf

from sherlock.common.config_util import load_default_config

from mycroft import log_util
from mycroft.backend.sqs_wrapper import SQSWrapper
from mycroft.backend.worker.etl_status_helper import ETLStatusHelper
from mycroft.log_util import log
from mycroft.log_util import log_exception
from mycroft.models.aws_connections import TableConnection
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_SCHEDULED
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_SUCCESS
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_COMPLETE
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_ERROR
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_RUNNING
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_CANCELLED
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_PAUSED
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_DELETED
from mycroft.models.scheduled_jobs import JOBS_ETL_ACTIONS
from mycroft.backend.util import parse_results


def worker_extended(inqueue, outqueue, initializer=None, initargs=(), maxtasks=None):
    """ This worker is mostly copy-and-paste from from multiprocessing.pool

    :param inqueue: input queue to fetch command to execute
    :type inqueue: SimpleQueue
    :param outqueue: output queue to place results after command is executed
    :type outqueue: SimpleQueue
    :param initializer: a function to perform custom initialization
    :type initializer: function pointer
    :param initargs: initialization arguments to pass to initializer
    :type initargs: list
    :param maxtasks: Not used, solely for 2.6, 2.7 compatablitiy
    :type maxtasks: int
    """
    put = outqueue.put
    get = inqueue.get
    if hasattr(inqueue, '_writer'):
        inqueue._writer.close()
        outqueue._reader.close()

    if initializer is not None:
        initializer(*initargs)

    while 1:
        try:
            task = get()
        except (EOFError, IOError):
            log_exception('worker got EOFError or IOError -- exiting')
            break
        except:
            log_exception('unknown exception')
            break

        if task is None:
            break

        job, i, func, args, kwds = task
        try:
            result = (True, func(*args, **kwds))
        except Exception, e:
            log_exception('exception in {0}({1}, {2})'.format(func, args, kwds))
            result = (False, e)
        put((job, i, result))


class ProcessExtended(Process):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}):
        if target == default_worker:
            # override default worker process with our own to have more control
            target = worker_extended
        super(ProcessExtended, self).__init__(group, target, name, args, kwargs)


class PoolExtended(Pool):
    Process = ProcessExtended

    def cancel(self):
        for worker in self._pool:
            # copied exception handling from multiprocessing.forking terminate()
            try:
                os.kill(worker._popen.pid, signal.SIGINT)
            except OSError:
                if worker._popen.wait(timeout=0.1) is None:
                    raise


class WorkerJob(object):
    """ An active job that worker is executing
    Subclasses must implement
    :func:`mycroft.backend.worker.base_worker.WorkerJob.has_more_runs_to_schedule'
    :func:`mycroft.backend.worker.base_worker.WorkerJob.schedule_next_run'
    :func:`mycroft.backend.worker.base_worker.WorkerJob.run_complete'
    Subclasses may need to implement
    :func:`mycroft.backend.worker.base_worker.WorkerJob.has_incomplete_runs'
    methods.  For more details, see doc string for each method
    """

    ACTION_REQUESTED_DICT = dict((action, None) for action in JOBS_ETL_ACTIONS)

    RunTuple = namedtuple('RunTuple', ['id', 'step', 'func', 'args'])

    def __init__(self, worker, msg_dict, max_runs_in_flight):
        self.worker = worker
        self.runs_done = 0
        self.runs_in_flight = 0
        self.max_runs_in_flight = max_runs_in_flight
        self.msg_dict = msg_dict
        self.all_results = defaultdict(list)
        self.is_waiting = False
        self.actions = {}
        self.cancel_in_progress = False
        self.pause_in_progress = False
        self.last_keepalive_update_sec = 0

    def update_action_requests(self):
        """  get the values of action requests from table """

        if self.cancel_in_progress and self.pause_in_progress:
            return
        job = self.worker._get_scheduled_job(self.msg_dict['hash_key'])
        result = job.get(**self.ACTION_REQUESTED_DICT)
        # convert the non-boolean value to boolean
        self.actions = dict((k, False if v in [0, None] else True) for k, v in result.items())

    def update_keepalive(self):
        """ Update scheduled jobs status from running to running, essentially
            updating timestamp field.  This tells scanner, worker is alive
        """
        now = time.time()
        if now - self.last_keepalive_update_sec < self.worker._wait_timeout_sec:
            return
        new_kwargs = {'et_status': JOBS_ETL_STATUS_RUNNING}
        self.worker._update_scheduled_jobs(self.msg_dict['hash_key'], new_kwargs)
        self.last_keepalive_update_sec = now

    def is_done(self):
        """ checks a set of rules to decide if job is done
        returns: True if job is done, False otherwise
        rtype: bool
        """
        if self.runs_in_flight != 0 or self.has_incomplete_runs():
            return False

        if self.cancel_in_progress:
            return True

        if self.pause_in_progress:
            return True

        if not self.has_more_runs_to_schedule():
            return True
        return False

    def has_incomplete_runs(self):
        """ Checks if multi-step runs have more steps to execute
        returns: True if more steps need to be executed, False otherwise

        Ensures that job does not stop execution while some runs have unfinished steps.

        For runs with single step, default implementation is enough.
        For runs consisting of multiple steps (ex: extract, transform, load),
        one must return False IF AND ONLY IF there are no runs with unfinished steps.
        See mycroft/backet/worker/et_worker.py for example
        """
        return False

    def has_more_runs_to_schedule(self):
        """ checks if there are more runs to schedule for
            execution

        returns: True if can schedule more runs, False otherwise
        rtype: bool
        """
        raise NotImplementedError

    def schedule_next_run(self):
        """ selects next run to execute
        returns: a run tuple(id, step, function, function_arguments)
        rtype: RunTuple
        """
        raise NotImplementedError

    def run_complete(self, run_id, step, results):
        """ Callback invoked when a single run is done

        :param job: current job
        :type: WorkerJob
        :param run_id: an unique run identifier
        :type: string
        :param step: step associated with the run execution
        :type: string
        :param results: run results
        :type: list
        """
        raise NotImplementedError


class BaseMycroftWorker(object):

    """ Worker hosted on mycroft AWS instances. Picks up work messages from
    SQS and runs the appropriate ET or Load scripts for the parameters
    (log name, schema, date, etc.) mentioned in these messages.
    Subclasses must implement
    :func:`mycroft.backend.worker.base_worker.BaseMycroftWorker.create_worker_job`
    methods.
    """

    def __init__(self, config_loc, config_override_loc, emailer,
                 num_processes=1, wait_timeout_sec=60):
        """
        :param config_loc: path of config.yaml
        :type config_loc: string

        :param config_override_loc: path of config-env-dev.yaml
        :type config_override_loc: string

        :param run_local: run local flag
        :type run_local: boolean

        :param num_processes: number of worker processes to use for sqs request
        :type num_processes: int

        :param wait_timeout_sec: A timeout passed to conditional variable wait
            function.  If thread is woken up on timeout, do some maintenance work.
        :type wait_timeout_sec: int

        """
        self._config_loc = config_loc
        self._config_override_loc = config_override_loc
        self._stop_requested = False
        self._run_once = False
        self.max_error_retries = staticconf.read_int('max_error_retries')
        self.etl_helper = ETLStatusHelper()
        self.jobs_db = TableConnection.get_connection('ScheduledJobs')
        self.runs_db = TableConnection.get_connection('ETLRecords')
        self._num_processes = num_processes
        self._cond = threading.Condition(threading.Lock())
        self._wait_timeout_sec = max(wait_timeout_sec, 60)
        self.emailer = emailer

    def stop(self):
        """ Stop a running worker.
        """
        self._stop_requested = True

    def _get_sqs_wrapper(self, queue_name, class_type):
        return SQSWrapper(queue_name, class_type)

    def run(self):
        """Main entry point for the worker. Queries an SQS queue for messages
        and performs the appropriate action on each message received.
        Swallows all exceptions and logs them.
        """
        queue_name = str(self._get_queue_name())
        sqs = self._get_sqs_wrapper(queue_name, JSONMessage)

        scanner_queue_name = str(self._get_scanner_queue_name())
        scanner_sqs = self._get_sqs_wrapper(scanner_queue_name, JSONMessage)
        dummy_message = {"message": "dummy"}  # TODO: make this message meaningful

        while(not self._stop_requested):  # Loop forever while this variable is set.
            try:  # Main try-except
                for msg in sqs.get_messages_from_queue():
                    msg_body = msg.get_body()
                    log({
                        "status": "new message",
                        "queue": queue_name,
                        "msg": msg_body,
                    })

                    results = None
                    final_status = JOBS_ETL_STATUS_ERROR
                    lsd = None
                    try:
                        self._update_scheduled_jobs_on_etl_start(msg_body)
                        # safe to delete message. if worker dies, scanner will resubmit
                        sqs.delete_message_from_queue(msg)

                        try:
                            # Execute etl
                            results, action_dict = self._process_msg(msg)

                            # Parse results
                            final_status, lsd, extra_info = \
                                parse_results(results, msg_body['end_date'])
                            if final_status != JOBS_ETL_STATUS_COMPLETE:
                                if action_dict['delete_requested']:
                                    final_status = JOBS_ETL_STATUS_DELETED
                                elif action_dict['cancel_requested']:
                                    final_status = JOBS_ETL_STATUS_CANCELLED
                                elif action_dict['pause_requested']:
                                    final_status = JOBS_ETL_STATUS_PAUSED

                            log({
                                "status": "processed message OK",
                                "queue": queue_name,
                                "msg": msg_body,
                                "results": results,
                                "job status": final_status,
                                "last OK date": lsd,
                            })
                        except Exception:
                            final_status = JOBS_ETL_STATUS_ERROR
                            log_exception(
                                "Exception in processing msg from queue: " +
                                queue_name + " msg body:" + str(msg_body)
                            )
                        if final_status != JOBS_ETL_STATUS_DELETED:
                            self._update_scheduled_jobs_on_etl_complete(
                                msg_body, final_status, lsd
                            )
                        scanner_sqs.write_message_to_queue(dummy_message)
                        try:
                            self.emailer.mail_result(
                                final_status, msg_body, additional_info=extra_info
                            )
                            log(
                                "Sent emails to:" + str(msg_body['contact_emails'])
                            )
                        except Exception:
                            log_exception(
                                "Exception in sending emails of job:" +
                                str(msg_body)
                            )
                    except Exception:
                        log_exception(
                            "Failed to update scheduled jobs on etl"
                            " start/complete, msg body: " + str(msg_body)
                        )
            except Exception:  # end of main try-except
                log_exception(
                    "Exception in fetching messages from queue:"
                    + queue_name
                )
                # if sqs queue fails, throttle retry
                time.sleep(sqs.get_wait_time())
            if self._run_once:
                break

        self._stop_requested = False

    def _update_scheduled_jobs_on_etl_start(self, msg_dict):
        """ Update scheduled jobs status from scheduled to running
        """
        new_kwargs = {"et_status": JOBS_ETL_STATUS_RUNNING}

        job = self._get_scheduled_job(msg_dict['hash_key'])
        job_status = job.get(**new_kwargs)['et_status']
        if job_status == JOBS_ETL_STATUS_SCHEDULED:
            self._update_scheduled_jobs(msg_dict['hash_key'], new_kwargs)
            job_status = new_kwargs['et_status']
        else:
            raise ValueError(
                "Unexpected job status {0} for job {1}".format(job_status, job.__dict__)
            )
        return job_status

    def _update_scheduled_jobs_on_etl_complete(
            self, msg_dict, final_status, lsd):
        """ Updates the scheduled jobs table with the result of this etl -
        both *_status and *_last_successul_date are updated - these are inturn
        used by the scanner to schedule the next run.
        """
        now = datetime.utcnow()

        new_kwargs = {
            'et_status': final_status,
            'et_status_last_updated_at': str(now),
        }
        if lsd is not None:
            new_kwargs['et_last_successful_date'] = lsd

        num_retries_field = 'et_num_error_retries'
        next_retry_field = 'et_next_error_retry_attempt'

        if final_status == JOBS_ETL_STATUS_ERROR:
            job = self.jobs_db.get(hash_key=msg_dict['hash_key'])
            args = {num_retries_field: -1}
            fields = job.get(**args)
            num_retries = int(fields[num_retries_field]) + 1

            if num_retries < self.max_error_retries:
                # exponential backoff
                next_retry = timedelta(seconds=(300 * (2 ** num_retries))) + now
                new_kwargs[next_retry_field] = str(next_retry)
            new_kwargs[num_retries_field] = num_retries
        elif final_status == JOBS_ETL_STATUS_SUCCESS:
            # If not reset, perpetual jobs will eventually reach max retries
            # and auto-retry will stops working.  Delete by setting to None
            new_kwargs[num_retries_field] = None
            new_kwargs[next_retry_field] = None

        self._update_scheduled_jobs(msg_dict['hash_key'], new_kwargs)

    def _get_scheduled_job(self, hash_key):
        job = self.jobs_db.get(hash_key=hash_key)
        if job is None:
            raise ValueError(
                "Could not find job entry for hash key {0}".format(hash_key)
            )
        return job

    def _update_scheduled_jobs(self, hash_key, new_kwargs):
        job = self._get_scheduled_job(hash_key)
        ret = job.update(**new_kwargs)
        if not ret:
            raise ValueError(
                "Could not update scheduled jobs entry for etl start/finish,"
                " hash key:" + hash_key + " return value: " + str(ret)
            )

    def _run_complete_callback(self, job, run_id, step, results):
        """ Callback invoked when a single run is done

        :param job: current job
        :type: WorkerJob
        :param run_id: an unique run identifier
        :type: string
        :param step: step associated with the run execution
        :type: string
        :param results: run results
        :type: list
        """
        self._cond.acquire()
        try:
            if len(results) != 1:
                raise ValueError("len(results) != 1, {0}".format(results))
            log("done: {0}, {1}, {2}".format(run_id, step, results[0]['status']))

            job.all_results[run_id].extend(results)
            job.runs_done += 1
            job.runs_in_flight -= 1

            if job.runs_in_flight < 0:
                raise ValueError("runs_in_flight < 0 \
    ({0} < 0)".format(job.runs_in_flight))

            if job.is_waiting is True:
                self._cond.notify(n=1)

            self.etl_helper.etl_step_complete(job.msg_dict, run_id, step, results[0])
            job.run_complete(run_id, step, results)
        except:
            # if callback dies, et_pool stops working
            log_exception('_run_complete_callback')
        finally:
            self._cond.release()

    def _create_run_complete_callback(self, job, run_id, step):
        return lambda results: self._run_complete_callback(
            job, run_id, step, results
        )

    def _schedule_runs_lk(self, et_pool, job):
        """ Schedule runs to execute up to max possible parallelism
        suffix '_lk' means caller must already hold lock.

        :param et_pool: A multiprocessor pool handle
        :type: Pool
        :param job: current job
        :type: WorkerJob
        """
        while (self._has_more_runs_to_schedule(job) and
               job.runs_in_flight < job.max_runs_in_flight):
            run = job.schedule_next_run()
            if run.id is None:
                raise ValueError("Unexpected end of runs")

            self.etl_helper.etl_step_started(job.msg_dict, run.id, run.step)

            log('scheduled: {0}'.format(run.id))
            et_pool.apply_async(
                run.func,
                args=run.args,
                callback=self._create_run_complete_callback(job, run.id, run.step),
            )
            job.runs_in_flight += 1

    def _handle_job_delete(self, job):
        """ Delete job. Start by deleting runs first.  If we die during deletion
        we can resume deletion until entry is deleted from jobs table
        """
        jobid = job.msg_dict['uuid']

        result = self.runs_db.delete_job_runs(jobid)
        if not result:
            raise Exception("failed to delete all runs for job {0}".format(jobid))

        result = self.jobs_db.delete(hash_key=job.msg_dict['hash_key'])
        if not result:
            raise Exception("failed to delete job {0}".format(jobid))

    def _handle_cancel_request_lk(self, job, et_pool):
        """ Cancel currently executing job.  Send SIGINT to all children,
        ignore SIGINT in parent to prevent killing it.

        Note: suffix '_lk' means caller must already hold lock.

        :param job: current job
        """
        log("cancel detected, terminating")
        if job.runs_in_flight != 0:
            log("sending cancel request")
            et_pool.cancel()

    def _has_more_runs_to_schedule(self, job):
        if job.has_incomplete_runs():
            return True

        return not job.pause_in_progress and job.has_more_runs_to_schedule()

    def create_worker_job(self, job_request):
        """ A factory method to create custom jobs
        :param job_request: job request message
        :type: json dict
        :returns: a custom job object
        :rtype:  WorkerJob
        """
        raise NotImplementedError

    def _process_msg(self, etl_msg):
        """ Process the given SQS message and run an appropriate step (ET/L).

        :param etl_msg: instance of `:class:boto.sqs.jsonmessage.JSONMessage`
        """
        msg_dict = etl_msg.get_body()

        job = self.create_worker_job(msg_dict)
        et_pool = PoolExtended(processes=self._num_processes)
        self._cond.acquire()
        try:
            while True:
                job.update_action_requests()

                if job.actions['delete_requested'] is True:
                    self._handle_job_delete(job)
                    break

                if not job.cancel_in_progress and job.actions['cancel_requested'] is True:
                    self._handle_cancel_request_lk(job, et_pool)
                    job.cancel_in_progress = True

                if not job.pause_in_progress and job.actions['pause_requested'] is True:
                    # pause the job by waiting for the completion of ongoing run(s) and refusing
                    # any new run.
                    job.pause_in_progress = True

                if job.is_done():
                    break

                if not job.cancel_in_progress:
                    self._schedule_runs_lk(et_pool, job)
                job.is_waiting = True
                self._cond.wait(self._wait_timeout_sec)
                job.update_keepalive()
                job.is_waiting = False
        finally:
            self._cond.release()
            et_pool.close()
            et_pool.join()
        return job.all_results, job.actions

    def _get_queue_name(self):
        """ Get the name of SQS queue to use for this worker.
        """
        raise NotImplementedError

    def _get_scanner_queue_name(self):
        """ Get the name of feedback scanner SQS queue to use for this worker.
        """
        raise NotImplementedError


def setup_config(args, tag):
    if args.config is not None:
        load_default_config(args.config, args.config_override)
    log_util.setup_pipeline_stream_logger(args.run_local, tag)  # one time setup
