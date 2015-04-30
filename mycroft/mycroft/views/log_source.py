# -*- coding: utf-8 -*-
from pyramid.view import view_config
from requests.exceptions import HTTPError

from mycroft.logic.log_source_action import search_log_source_by_keyword


@view_config(route_name='api.log_source.search', request_method='POST',
             renderer='json_with_status')
def log_source_search(request):
    """
    log_source_serach handles requests from the log_source endpoint to search for logs
    beginning with given keyword.

    **GET /v1/log_source/search**

    Example: ``/v1/log_source/search``

    **Query Parameters:**

    * **request.body** -- the json string of log keyword

    *Example request.body* ::

        "{ 'keyword': 'foo',
        }"

    *Example Response* ::

        [
            {'bucket': 'bucket_name',
             'log_name': 'foo'
             'uri': 's3://bucket_name/logs/foo/'
            },
            {'bucket': 'bucket_name',
             'log_name': 'foo_bar'
             'uri': 's3://bucket_name/logs/foo_bar/'
            },
        ]

    ============ ===========
    Status Code  Description
    ============ ===========
    **200**      success
    **404**      invalid value(s)
    **500**      unknown exception
    ============ ===========

    * **Encoding type:** *application/json*
    """

    try:
        return 200, search_log_source_by_keyword(request.body)
    except ValueError as e:
        return 404, {'error': repr(e)}
    except HTTPError as e:
        return 404, {'error': repr(e)}
    except Exception as unknown_exception:
        return 500, {'error': repr(unknown_exception)}
