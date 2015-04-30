# -*- coding: utf-8 -*-
"""
**views.runs**
==============

This module contains the runs function that handles requests for the runs
endpoint of the mycroft service.
"""
from pyramid.view import view_config

from mycroft.logic.run_actions import list_runs_by_job_id

from mycroft.models.aws_connections import TableConnection


@view_config(route_name='api.runs_job_id', request_method='GET', renderer='json_with_status')
def runs_filtered(request):
    """
    runs_filtered handles requests from the runs endpoint with a job_id.  All
    runs for the job_id requested are returned.

    **GET /v1/runs/**\ *{string: job_id}*

    **Query Parameters:**

    * **job_id** - the job_id of the runs we wish to review

    Example: ``/v1/runs/234332104332``

    *Example Response* ::

        [
            {
             ‘job_id’: <string>,
             ‘etl_status’: <string>,
             ‘last_updated’: timestamp,
             ‘data_date’: ‘YYYY-mm-dd’,
             ‘schema_checksum’: <string>,
             ‘s3_path’: <string>,
             ‘et_starttime’: timestamp,
             ‘et_runtime’: int,
             ‘load_starttime’: timestamp,
             ‘load_runtime’: int,
             ‘redshift_id’: <string>,
             ‘db_schema’: <string>,
             ‘run_by’: <string>,
             'additional_arguments': <string>
            }
        ]

    ============ ===========
    Status Code  Description
    ============ ===========
    **200**      Success
    **404**      Invalid job_id
    **500**      unknown exception
    ============ ===========

    * **Encoding type:** *application/json*
    """
    job_id = request.matchdict.get('job_id', None)

    try:
        return 200, list_runs_by_job_id(job_id, TableConnection.get_connection('ETLRecords'))
    except ValueError as e:
        return 404, {'error': repr(e)}
    except Exception as unknown_exception:
        return 500, {'error': repr(unknown_exception)}
