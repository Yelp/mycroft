# -*- coding: utf-8 -*-
from datetime import datetime
import os
import socket

from mycroft.log_util import log_debug
from mycroft.models.aws_connections import TableConnection


class ETLStatusHelper(object):

    """ A helper to record etl status into ETLRecords dynamodb table.
    Constructs appropriate entries from a given SQS message dict and inserts
    into the table.
    """

    def __init__(self):
        self.etl_db = TableConnection.get_connection('ETLRecords')
        self.worker_id = '{0}:{1}'.format(socket.gethostname(), os.getpid())

    def _init_args_from_msg_dict(self, msg_dict, status=None):
        return {
            'updated_at': str(datetime.utcnow()),
            # TODO: Add worker id strings (hostname?)
            'run_by': msg_dict.get('run_by', self.worker_id),
            'redshift_id': msg_dict['redshift_id'],
            'hash_key': msg_dict['uuid'],
            'job_id': msg_dict['uuid'],
            'etl_status': status,
        }

    def etl_step_started(self, msg_dict, run_id, step):
        """ Mark runs as started in etl status db. Attempts to
        insert all entries and raises ValueError in case of any failures.

        :param msg_dict: dict representing job from an SQS message
        :type msg_dict: dict
        :param run_id: a unique per job run identifier string
        :type run_id: string
        :param step: type of work initiated for these records
        :type step: string
        """
        new_args = self._init_args_from_msg_dict(
            msg_dict, status=step + '_started'
        )
        new_args['{0}_starttime'.format(step)] = new_args['updated_at']
        new_args['data_date'] = run_id

        existing_run = [
            run for run in self.etl_db.get_runs_with_job_id(
                msg_dict['uuid'], run_id
            )
        ]
        if len(existing_run) == 0:
            ret = self.etl_db.put(**new_args)
            log_debug(
                "Added entry on etl start: {0}, return: {1}".format(
                    new_args, str(ret)
                )
            )
        elif len(existing_run) == 1:
            ret = existing_run[0].update(**new_args)
            log_debug(
                "Updated entry on etl start: {0}, return: {1}".format(
                    new_args, str(ret)
                )
            )
        else:
            raise ValueError("Expected 0 or 1 entries in runs")

    def etl_step_complete(self, msg_dict, run_id, step, result):
        """ Mark the completion of et/l work in etl status db. Attempts to
        insert all entries and raises ValueError in case of any failures.

        :param msg_dict: dict representing job from an SQS message
        :type msg_dict: dict
        :param run_id: a unique per job run identifier string
        :type run_id: string
        :param step: type of work initiated for these records
        :type step: string
        :param result: a dict of form {'status': ..., 'error_info': ...})
        :type status_records: dict
        """
        new_args = self._init_args_from_msg_dict(msg_dict)
        etl_err = str(result.get('error_info')) if result['status'] != 'success' else None

        # Add or update an entry in etl records table
        existing_run = [
            run for run in self.etl_db.get_runs_with_job_id(
                msg_dict['uuid'], run_id
            )
        ]

        # Update fields from the record
        new_args.update({
            'etl_status': step + '_' + result['status'],
            'data_date': run_id,
            'etl_error': etl_err,
        })

        if len(existing_run) > 1:
            raise ValueError("Expected 0 or 1 entries in runs")

        if len(existing_run) == 1:
            existing_run = existing_run[0]
            end_time_str = new_args['updated_at']

            kwargs = {step + '_starttime': None}
            start_time_str = existing_run.get(**kwargs).get(step + '_starttime', end_time_str)

            start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S.%f')
            end_time = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S.%f')

            td = end_time - start_time
            runtime_secs = td.days * 86400 + td.seconds + td.microseconds / 1000000.0
            new_args.update({step + '_runtime': str(runtime_secs)})
            ret = existing_run.update(**new_args)
        else:
            new_args.update({step + '_runtime': 0})
            ret = self.etl_db.put(**new_args)
        log_debug(
            "Updated entry on etl complete: {0}, return: {1}".format(
                new_args, str(ret)
            )
        )
