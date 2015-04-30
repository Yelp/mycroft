# -*- coding: utf-8 -*-
"""
**views.jobs**
================

This module contains the jobs function that handles requests for the jobs
endpoint of the mycroft service.
"""
from simplejson.decoder import JSONDecodeError
from staticconf import read_string

from pyramid.view import view_config

from mycroft.logic.job_actions import list_all_jobs
from mycroft.logic.job_actions import list_jobs_by_name
from mycroft.logic.job_actions import list_jobs_by_name_version
from mycroft.logic.job_actions import post_job
from mycroft.logic.job_actions import put_job

from mycroft.models.aws_connections import TableConnection
from mycroft.models.abstract_records import PrimaryKeyError

from mycroft.backend.sqs_wrapper import SQSWrapper


def get_scanner_queue(etl_type):
    """
    Return the scanner sqs for jobs to send a message when post a job
    to wake up the scanner
    :param etl_type: et or load
    :type etl_type: string in ['et', 'load']
    """
    return SQSWrapper(read_string("sqs.{0}_scanner_queue_name".format(etl_type)))


@view_config(route_name='api.jobs', request_method='GET', renderer='json_with_status')
@view_config(route_name='api.jobs', request_method='POST', renderer='json_with_status')
def jobs(request):
    """
    jobs_name_and_version handles requests from the jobs endpoint with
    log_name and log_version, getting contents from the dynamo location

    **GET /v1/jobs/**

    Example: ``/v1/jobs/``

    *Example Response* ::

        [
            {'log_name': 'ad_click',
             'log_schema_version': 'initial',
             's3_log_uri': http://ad_click/schema.yaml?Signature=b?Expires=c?AccessKeyId=xxx
             'start_date': '2014-05-01',
             'end_date': '',
             'contact_emails': ['you@company.com', 'dev_null@company.com'],
             'redshift_id': 'abc123',
             'additional_arguments': '{"et_step": ["--force-et"]}'
            },
            {'log_name': 'ad_click',
             'log_schema_version': 'minimal',
             's3_log_uri': http://ad_min/schema.yaml?Signature=b?Expires=b?AccessKeyId=yyy
             'start_date': '2014-05-01',
             'end_date': '2014-05-07',
             'contact_emails': ['you@company.com', 'dev_null@company.com'],
             'redshift_id': 'abc123'
             'additional_arguments': '{"et_step": ["--force-et"]}'
            },
            {'log_name': 'bing_geocoder',
             'log_schema_version': 'bing2',
             's3_log_uri': http://bing/schema.yaml?Signature=b?Expires=a?AccessKeyId=zzz
             'start_date': '2014-05-02',
             'end_date': '2014-06-07',
             'contact_emails': ['you@company.com', 'dev_null@company.com'],
             'redshift_id': 'abc123'
             'additional_arguments': '{"et_step": ["--force-et"]}'
            }
        ]


    ============ ===========
    Status Code  Description
    ============ ===========
    **200**      Success
    **500**      unknown exception
    ============ ===========

    * **Encoding type:** *application/json*

    **POST /v1/jobs/**

    Example: ``v1/jobs``

    **Query Parameters:**

    * **request.body** -- the json string of job details

    *Example request.body* ::

        "{ 'log_name': 'ad_click',
           'log_schema_version': 'initial',
           's3_log_uri': 'llll',
           'start_date': '2014-04-01',
           'end_date': '',
           'contact_emails': ['devnull@company.com', 'you@company.com'],
           'redshift_id': 'rs1',
           'additional_arguments': '{"load_step": ["--force-load"]}'
        }"

    ============ ===========
    Status Code  Description
    ============ ===========
    **200**      Success
    **400**      bad hash_key: redshift_id, log_name,
                 log_schema_version and start_date must all be present
    **404**      invalid job parameters
    **500**      unknown exception
    ============ ===========

    * **Encoding type:** *application/json*
    """

    try:
        if request.method == "POST":
            return 200, post_job(TableConnection.get_connection('ScheduledJobs'),
                                 get_scanner_queue('et'),
                                 request.body)
        elif request.method == "GET":
            return 200, list_all_jobs(TableConnection.get_connection('ScheduledJobs'))
    except PrimaryKeyError as e:
        return 400, {'error': 'bad hash_key'}
    except ValueError as e:
        if "ConditionalCheckFailedException" in repr(e):
            return 404, {'error': "ConditionalCheckFailed; possible duplicate job.  \
Delete existing job first"}
        return 404, {'error': repr(e)}
    except Exception as unknown_exception:
        return 500, {'error': repr(unknown_exception)}


@view_config(route_name='api.jobs_log_name', request_method='GET',
             renderer='json_with_status')
@view_config(route_name='api.jobs_log_name_version', request_method='GET',
             renderer='json_with_status')
def jobs_filtered(request):
    """
    jobs_filtered handles requests from the jobs endpoint with a log_name and
    optional version.  If there's no version all jobs will be for the given
    log_name will be returned, otherwise all jobs for the log name and version
    combination will be returned.

    **GET /v1/jobs/**\ *{string: log_name}*

    **Query Parameters:**

    * **log_name** - the name of the log for which we want to see jobs

    Example: ``/v1/jobs/ad_click``

    *Example Response* ::

        [
            {'log_name': 'ad_click',
             'log_schema_version': 'initial',
             's3_log_uri': http://ad_click/schema.yaml?Signature=b?Expires=c?AccessKeyId=xxx
             'start_date': '2014-05-01',
             'end_date': '',
             'contact_emails': ['you@company.com', 'dev_null@company.com'],
             'redshift_id': 'abc123',
             'additional_arguments': '{"load_step": ["--force-load"]}'
            },
            {'log_name': 'ad_click',
             'log_schema_version': 'minimal',
             's3_log_uri': http://ad_min/schema.yaml?Signature=b?Expires=b?AccessKeyId=yyy
             'start_date': '2014-05-01',
             'end_date': '2014-05-07',
             'contact_emails': ['you@company.com', 'dev_null@company.com'],
             'redshift_id': 'abc123',
             'additional_arguments': '{"load_step": ["--force-load"]}'
            }
        ]

    ============ ===========
    Status Code  Description
    ============ ===========
    **200**      Success
    **404**      invalid log_name
    **500**      unknown exception
    ============ ===========


    **GET /v1/jobs/**\ *{string: log_name}/{string: log_schema_version}*

    **Query Parameters:**

    * **log_name** - the name of the log for which we want to see jobs
    * **log_schema_version** - the version of the log for which we want to see jobs

    Example: ``/v1/jobs/ad_click/initial``

    *Example Response* ::

        [
            {'log_name': 'ad_click',
             'log_schema_version': 'initial',
             's3_log_uri': http://ad_click/schema.yaml?Signature=b?Expires=c?AccessKeyId=xxx
             'start_date': '2014-05-01',
             'end_date': '',
             'emails': ['you@company.com', 'dev_null@company.com'],
             'redshift_id': 'abc123',
             'additional_arguments': '{"et_step": ["--force-et"]}'
            }
        ]

    ============ ===========
    Status Code  Description
    ============ ===========
    **200**      Success
    **404**      invalid log_name or log_version
    **500**      unknown exception
    ============ ===========

    * **Encoding type:** *application/json*
    """
    log_name = request.matchdict.get('log_name')
    log_version = request.matchdict.get('log_schema_version', None)

    try:
        if log_version is None:
            return 200, list_jobs_by_name(log_name,
                                          TableConnection.get_connection('ScheduledJobs'))
        return 200, list_jobs_by_name_version(log_name,
                                              log_version,
                                              TableConnection.get_connection('ScheduledJobs'))
    except ValueError as e:
        return 404, {'error': repr(e)}
    except Exception as unknown_exception:
        return 500, {'error': repr(unknown_exception)}


@view_config(route_name='api.jobs_job_id', request_method='PUT', renderer='json_with_status')
def jobs_update_job(request):
    """
    jobs_update_job_by_job_id handles requests from the jobs endpoint.

    **PUT /v1/jobs/job/**

    Example: ``v1/jobs/job/``

    **Query Parameters:**

    * **request.body** -- the json string of job details

    *Example request.body* ::

        "{ 'log_name': 'ad_click',
           'log_schema_version': 'initial',
           'start_date': '2014-04-01',
           'end_date': '',
           'redshift_id': 'rs1',
           'cancel_requested': True,
        }"

    ============ ===========
    Status Code  Description
    ============ ===========
    **200**      Success
    **400**      bad hash_key: redshift_id, log_name,
                 log_schema_version and start_date must all be present
    **404**      invalid job parameters
    **500**      unknown exception
    ============ ===========

    * **Encoding type:** *application/json*
    """
    try:
        return 200, put_job(TableConnection.get_connection('ScheduledJobs'),
                            get_scanner_queue('et'),
                            request.body)
    except PrimaryKeyError as e:
        return 400, {'error': 'bad hash_key'}
    except JSONDecodeError as e:
        return 400, {'error': 'json decode error'}
    except ValueError as e:
        return 404, {'error': repr(e)}
    except Exception as unknown_exception:
        return 500, {'error': repr(unknown_exception)}
