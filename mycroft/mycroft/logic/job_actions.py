# -*- coding: utf-8 -*-
"""
**logic.job_actions**
========================

A collection of functions to handle actions relating to jobs
in the mycroft service.  The C part of MVC for mycroft/jobs
"""

import simplejson
import re
import uuid
import datetime

from mycroft.models.abstract_records import PrimaryKeyError
from mycroft.logic.cluster_actions import list_cluster_by_name
from mycroft.models.aws_connections import TableConnection
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUSES_FINAL
from mycroft.models.scheduled_jobs import JOBS_ETL_ACTIONS


NAME_RE = re.compile("[\w]+$")
HASH_KEY = "{redshift_id}:{log_name}:{log_schema_version}:{start_date}:{end_date}"
HASH_KEY_NO_END = "{redshift_id}:{log_name}:{log_schema_version}:{start_date}:None"

ACTION_TO_REQUIRED_ARGS = {
    'post': frozenset([
        "log_name",
        "log_schema_version",
        "s3_path",
        "start_date",
        "redshift_id",
        "contact_emails"
    ]),
    'put': frozenset([
        "log_name",
        "log_schema_version",
        "start_date",
        "redshift_id",
    ])
}

SCHEDULED_JOB_KWARGS = {
    'log_name': None,
    'log_schema_version': None,
    's3_path': None,
    'start_date': None,
    'end_date': None,
    'contact_emails': None,
    'redshift_id': None,
    'uuid': None,
    'et_status': None,
    'load_status': None,
    'delete_requested': None,
    'cancel_requested': None,
    'pause_requested': None,
    'additional_arguments': None,
}

UUID_TRIES = 2  # times to obtain a non-conflicting UUID
NULL = "null"  # used for initialization of job status


def _parse_jobs(query_result):
    """
    _parse_jobs converts the results of a query on the backing store
    scheduled_jobs_object to a dictionary with a key of 'jobs' and value of a
    list of jobs

    *Each job returned in the list is of the form*::

        { 'log_name': 'ad_click',
          'log_schema_version': 'initial',
          's3_path': 'llll',
          'start_date': '2014-04-01',
          'end_date': '',
          'contact_emails': ['devnull@company.com', 'you@company.com'],
          'redshift_id': 'rs1',
          'uuid': '723edc9dac96437094ea420f2f9aee70',  # a hex uuid4()
          'et_status': 'complete',
          'additional_arguments': '{"et_step": ["--force-et"]}'
        }


    :param query_result: iterable of ScheduledJob elements
    :type query_result: iterable

    :returns: A dict of \{'jobs': [job1, job2, ...]\}
    :rtype: dict

    """
    def construct_job_dict(job_object):
        job_dict = job_object.get(**SCHEDULED_JOB_KWARGS)
        contact_emails = job_dict.get('contact_emails', None)
        if contact_emails:
            job_dict['contact_emails'] = list(contact_emails)
        return job_dict

    return {'jobs': [construct_job_dict(job) for job in query_result]}


def list_all_jobs(scheduled_jobs_object):
    """
    lists all jobs

    *Each job returned in the list is of the form*::

        { 'log_name': 'ad_click',
          'log_schema_version': 'initial',
          's3_path': 'llll',
          'start_date': '2014-04-01',
          'end_date': '',
          'contact_emails': ['devnull@company.com', 'you@company.com'],
          'redshift_id': 'rs1',
          'uuid': '723edc9dac96437094ea420f2f9aee70',  # a hex uuid4()
          'et_status': 'complete',
          'additional_arguments': '{"et_step": ["--force-et"]}'
        }

    :param scheduled_jobs_object: the ScheduledJobs from which we read jobs
    :type scheduled_jobs_object: an instance of ScheduledJobs

    :returns: A dict of \{'jobs': [job1, job2, ...]\}
    :rtype: dict

    """
    all_jobs = [job for job in scheduled_jobs_object]
    return _parse_jobs(all_jobs)


def list_jobs_by_name(log_name, scheduled_jobs_object):
    """
    lists all jobs for a particular log_name

    *Each job returned in the list is of the form*::

        { 'log_name': 'ad_click',
          'log_schema_version': 'initial',
          's3_path': 'llll',
          'start_date': '2014-04-01',
          'end_date': '',
          'contact_emails': ['devnull@company.com', 'you@company.com'],
          'redshift_id': 'rs1',
          'uuid': '723edc9dac96437094ea420f2f9aee70',  # a hex uuid4()
          'et_status': 'complete',
          'additional_arguments': '{"et_step": ["--force-et"]}'
        }

    :param log_name: name of the log (e.g., ad_click)
    :type log_name: string
    :param scheduled_jobs_object: the ScheduledJobs from which we read jobs
    :type scheduled_jobs_object: an instance of ScheduledJobs

    :returns: a dict of {'jobs': [job1, job2, ...]}
    :rtype: dict
    """
    if log_name is None or NAME_RE.match(log_name) is None:
        raise ValueError("invalid log_name")

    filtered_jobs = scheduled_jobs_object.get_jobs_with_log_name(log_name)
    return _parse_jobs(filtered_jobs)


def list_jobs_by_name_version(log_name, log_version, scheduled_jobs_object):
    """
    lists all jobs for a particular log_name and log_version

    *Each job returned in the list is of the form*::

        { 'log_name': 'ad_click',
          'log_schema_version': 'initial',
          's3_path': 'llll',
          'start_date': '2014-04-01',
          'end_date': '',
          'contact_emails': ['devnull@company.com', 'you@company.com'],
          'redshift_id': 'rs1',
          'et_status': 'complete',
          'additional_arguments': '{"et_step": ["--force-et"]}'
        }

    :param log_name: name of the log (e.g., ad_click)
    :type log_name: string
    :param scheduled_jobs_object: the ScheduledJobs from which we read jobs
    :type scheduled_jobs_object: an instance of ScheduledJobs

    :returns: A dict of {'jobs': [job1, job2, ...]}
    :rtype: dict
    """
    if log_name is None or NAME_RE.match(log_name) is None:
        raise ValueError("invalid log_name")
    if log_version is None or NAME_RE.match(log_version) is None:
        raise ValueError("invalid log_version")

    filtered_jobs = scheduled_jobs_object.get_jobs_with_log_name(
        log_name,
        log_schema_version=log_version
    )
    return _parse_jobs(filtered_jobs)


def _get_uuid(scheduled_jobs_object):
    current_jobs = list_all_jobs(scheduled_jobs_object)
    current_job_uuids = [job.get('uuid', None) for job in current_jobs['jobs']]
    trial_uuids = [uuid.uuid4().hex for i in range(UUID_TRIES)]
    for trial_uuid in trial_uuids:
        if trial_uuid not in current_job_uuids:
            return trial_uuid
    raise ValueError("all uuid's {0} conflicted with existing records".format(trial_uuids))


def _create_hash_key(param_dict):
    """
    :param param_dict: parameters to enter into the backing store
    :type param_dict: dictionary

    :returns: '<redshift_id>:<log_name>:<schema_version>:<start_date>:<end_date>'
    :rtype: string
    """

    try:
        if 'end_date' in param_dict and param_dict['end_date'] is not None:
            hash_key = HASH_KEY.format(**param_dict)
        else:
            hash_key = HASH_KEY_NO_END.format(**param_dict)
        return hash_key
    except KeyError:
        raise PrimaryKeyError


def _check_required_args(param_dict, op):
    """
    :param param_dict: parameters to enter into the backing store
    :type param_dict: dictionary
    :param op: operation type
    :type op: string

    :returns: None
    :rtype: None
    """
    if not ACTION_TO_REQUIRED_ARGS[op].issubset(set(param_dict)):
        raise PrimaryKeyError


def _validate_additional_args(param_dict):
    """
    :param param_dict: parameters to enter into the backing store
    :type param_dict: dictionary

    :returns: a string that tidies up the additional arguments
    :rtype: string
    """
    if 'additional_arguments' not in param_dict:
        return "{}"

    param_string = param_dict['additional_arguments'].strip()
    if not param_string:
        return "{}"

    try:
        simplejson.loads(param_string)
        return param_string
    except:
        raise ValueError("bad structure or values in additional arguments")


def post_job(scheduled_jobs_object, et_scanner_sqs, request_body_str):
    """
    the request body should be a dictionary (so \*\*request_body are kwargs),
    and it with the following required keys:

    * redshift_id
    * log_name
    * log_schema_version
    * start_date
    * s3_path
    * contact_emails

    :param scheduled_jobs_object: the ScheduledJobs to which we post jobs
    :type scheduled_jobs_object: an instance of ScheduledJobs
    :param et_scanner_sqs: the scanner sqs to send message to
    :type et_scanner_sqs: SQSWrapper object
    :param request_body_str: a string version of the request body dict
    :type request_body_str: string
    :param scheduled_jobs_object:  an instance of ScheduledJobs
    :type scheduled_jobs_object: ScheduledJobs

    :returns: success
    :rtype: boolean

    :raises S3ResponseError: if the bytes written don't match the length of
        the content
    """
    request_body_dict = simplejson.loads(request_body_str)
    if 'contact_emails' in request_body_dict and request_body_dict['contact_emails'] is not None:
        request_body_dict['contact_emails'] = set(request_body_dict['contact_emails'])
    _check_required_args(request_body_dict, 'post')
    s_date = datetime.datetime.strptime(request_body_dict['start_date'], "%Y-%m-%d")
    if 'end_date' not in request_body_dict:
        request_body_dict['end_date'] = None
    elif request_body_dict['end_date'] is not None:
        e_date = datetime.datetime.strptime(request_body_dict['end_date'], "%Y-%m-%d")
        if s_date > e_date:
            raise ValueError("start date should not be greater than end date")

    if not request_body_dict.get('log_format'):
        request_body_dict['log_format'] = 'json'

    request_body_dict['additional_arguments'] = _validate_additional_args(request_body_dict)

    # check that redshift cluster exists, throws ItemNotFound
    list_cluster_by_name(
        TableConnection.get_connection('RedshiftClusters'),
        request_body_dict['redshift_id']
    )

    request_body_dict['hash_key'] = _create_hash_key(request_body_dict)
    request_body_dict['uuid'] = _get_uuid(scheduled_jobs_object)
    request_body_dict['et_status'] = NULL
    ret = scheduled_jobs_object.put(**request_body_dict)
    if ret:
        dummy_message = {'message': 'dummy'}  # TODO: use meaningful message instead of dummy
        et_scanner_sqs.write_message_to_queue(dummy_message)
    return {'post_accepted': {'result': ret, 'uuid': request_body_dict['uuid']}}


def _process_action_requested(request_body_dict):
    """
    _process_action_requested explicitly check the input type for JOBS_ETL_ACTIONS
    It also converts boolean values to integer and insert timestamp.

    :param request_body_dict: contains the parameters of job for PUT
    :type request_body_dict: dict
    """
    for action in JOBS_ETL_ACTIONS:
        if action in request_body_dict:
            if type(request_body_dict[action]) is not bool:
                raise ValueError("type error for ", action)
            request_body_dict[action] = int(request_body_dict[action])
            action_timestamp_str = action + "_put_at"
            request_body_dict[action_timestamp_str] = str(datetime.datetime.utcnow())


def put_job(scheduled_jobs_object, et_scanner_sqs, request_body_str):
    """
    the request body should be a dictionary (so \*\*request_body are kwargs),
    and it with the following required keys:

    * log_name
    * log_schema_version
    * redshift_id
    * start_date

    :param scheduled_jobs_object: the ScheduledJobs to which we post jobs
    :type scheduled_jobs_object: an instance of ScheduledJobs
    :param et_scanner_sqs: the et scanner sqs to send message to
    :type et_scanner_sqs: SQSWrapper object
    :param request_body_str: a string version of the request body dict
    :type request_body_str: string

    :returns: success
    :rtype: boolean
    """
    request_body_dict = simplejson.loads(request_body_str)
    _check_required_args(request_body_dict, 'put')
    hash_key = _create_hash_key(request_body_dict)
    job = scheduled_jobs_object.get(hash_key=hash_key)
    et_status = job.get(et_status=None)['et_status']

    if request_body_dict.get('delete_requested'):
        if et_status not in JOBS_ETL_STATUSES_FINAL:
            raise ValueError("delete is not supported when job is {0}".format(et_status))
        # reset state so that scanner can discover it.
        # update is safe when state is in JOBS_ETL_STATUSES_FINAL
        request_body_dict['et_status'] = NULL

    _process_action_requested(request_body_dict)

    ret = job.update(**request_body_dict)
    if ret:
        notification_message = {'message': 'job put request'}
        et_scanner_sqs.write_message_to_queue(notification_message)

    return {'put_accepted': ret}
