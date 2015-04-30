# -*- coding: utf-8 -*-
import sys
from base_scanner import BaseScanner
from base_scanner import parse_cmd_args
from mycroft.backend.email import Mailer
from mycroft.backend.sqs_wrapper import SQSWrapper
from mycroft.backend.worker.base_worker import setup_config
from mycroft.log_util import log
from mycroft.models.aws_connections import TableConnection
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUSES_SCHEDULABLE
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_SCHEDULED
from staticconf import read_string
from staticconf import read_int
from mycroft.backend.util import next_date_for_string


class ETScanner(BaseScanner):

    """ A scanner for jobs that have pending ET work. Gets appropriate jobs from
    DDB and creates work items for them in SQS.
    """

    def __init__(self, db_obj, sqs_scanner_queue, sqs_worker_queue, emailer):
        super(ETScanner, self).__init__(db_obj, sqs_scanner_queue, sqs_worker_queue, emailer)

    def _fetch_jobs_for_work(self):
        # query self.db and return jobs
        statuses = JOBS_ETL_STATUSES_SCHEDULABLE
        result = [
            job for s in statuses for job in self.db.get_jobs_with_et_status(s)
        ]
        log(
            "ET scanner: num jobs to process from DB:" + str(len(result)))
        return result

    def _should_process_job(self, job):
        if not super(ETScanner, self)._should_process_job(job):
            return False
        et_lsd = job.get(et_last_successful_date=None)['et_last_successful_date']
        if et_lsd is not None \
                and not self._data_available_for_date(job, next_date_for_string(et_lsd)):
            return False
        return True

    def _update_job_status_conditionally(self, job):
        # TODO no conditional updates?
        return job.update(et_status=JOBS_ETL_STATUS_SCHEDULED)

    def _create_work_for_job(self, job):
        # Update status = SCHEDULED
        # iff the status is not SCHEDULED/COMPLETE/ERROR
        job_dict = job.get(**self.DEFAULT_KEYS_TO_FETCH_FROM_JOB)
        if not self._update_job_status_conditionally(job):
            log(
                "ET: Could not conditionally update job to status 'scheduled'"
                ".Skipping this job this time : " + str(job)
            )
            return

        start_date = self._later_date(
            job_dict['start_date'],
            next_date_for_string(job_dict['et_last_successful_date'])
        )
        end_date = self._earlier_date(job_dict['end_date'], self._get_max_available_date(job))
        self._enqueue_work_in_sqs(start_date, end_date, "ET", job_dict)
        log("Enqueued ET work item in SQS, for job: " + str(job_dict))

    def _get_timeout(self):
        return read_int("scanner.et_timeout")


def et_scanner_main(args):
    """ Create an instance of ETScanner and run it once.
    """
    setup_config(args, 'ETScanner')
    sqs_scanner_queue = SQSWrapper(read_string("sqs.et_scanner_queue_name"))
    sqs_worker_queue = SQSWrapper(read_string("sqs.et_queue_name"))
    scanner = ETScanner(TableConnection.get_connection('ScheduledJobs'),
                        sqs_scanner_queue, sqs_worker_queue, Mailer(args.run_local))
    scanner.run()

if __name__ == "__main__":
    args = parse_cmd_args(sys.argv)
    et_scanner_main(args)
