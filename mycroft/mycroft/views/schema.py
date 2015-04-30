# -*- coding: utf-8 -*-
"""
**views.schema**
================

This module contains the schema function that handles requests for the schema
endpoint of the mycroft service.
"""
from boto.s3.connection import S3ResponseError
from pyramid.view import view_config

from mycroft.logic.schema_actions import get_key_url
from mycroft.logic.schema_actions import list_versions_by_log_name
from mycroft.logic.schema_actions import list_all_log_versions
from mycroft.logic.schema_actions import post_key
from mycroft.logic.schema_actions import s3_bucket_action


@view_config(route_name='api.schema_log_name_and_version', request_method='GET',
             renderer='json_with_status')
@view_config(route_name='api.schema_log_name_and_version', request_method='POST',
             renderer='json_with_status')
def schema_name_and_version(request):
    """
    schema_name_and_version handles requests from the schema endpoint with
    log_name and log_version, both getting contents from the s3 location
    based on those data, and saving information to the s3 location.

    **GET /v1/schema/**\ *(string: log_name)/(string: log_version)*

    **Query Parameters:**

    * **log_name** - the name of the log
    * **log_version** - the version of the log

    Example: ``/v1/schema/ad_click/initial``

    *Example Response* ::

        {
            "log_name": "ad_click",
            "versions": "initial",
            "url": "http://bucket/k1/k2/schema.yaml?Signature=s?Expires=t?AccessKeyId=akid..."
        }

    ============ ===========
    Status Code  Description
    ============ ===========
    **200**      Success
    **404**      No document found for this log_name, log_version
    **500**      unknown exception
    **502**      invalid log_name or log_version
    ============ ===========

    * **Encoding type:** *application/json*

    **POST /v1/schema/**\ *(string: log_name, string: log_version)*

    Example: ``v1/schema/ad_click/next_version``

    **Query Parameters:**

    * **log_name** - the name of the log
    * **log_version** - the version of the log
    * **request.body** -- the content to be posted

    *Example request.body* ::

        tables:
           search:
             columns:
               - is_foreign: true
                 log_key: session_info.client
                 name: client
                 sql_attr: varchar(13)
              src: search.views
              src_type: list
        version: 13

    ============ ===========
    Status Code  Description
    ============ ===========
    **200**      Success
    **404**      invalid log_version or log_name
    **500**      unknown exception
    **502**      post incomplete
    ============ ===========

    * **Encoding type:** *application/json*
    """
    log_name = request.matchdict.get('log_name')
    log_version = request.matchdict.get('version')

    try:
        if request.method == "POST":
            return 200, s3_bucket_action(
                post_key,
                log_name,
                log_version,
                request.body
            )
        elif request.method == "GET":
            return 200, s3_bucket_action(get_key_url, log_name, log_version)
    except S3ResponseError as e:
        return e.status, {'error': e.reason}
    except ValueError as e:
        return 404, {'error': e.message}
    except Exception as unknown_exception:
        return 500, {'error': repr(unknown_exception)}


@view_config(route_name='api.schema_log_name', request_method='GET', renderer='json_with_status')
@view_config(route_name='api.schema', request_method='GET', renderer='json_with_status')
def schema(request):
    """
    schema handles requests from the schema endpoint with an optional log_name.
    If there's no log_name all schemas will be returned, otherwise all schema
    versions for the specified log_name are returned.

    **GET /v1/schema**

    Example: ``/v1/schema``

    *Example Response* ::

        {
            "schemas" : [
                {
                    "log_name": "ad_click",
                    "versions": ["initial"]
                },
                {
                    "log_name": "badge_updates",
                    "versions": ["initial"]
                }
            ]
        }


    **GET /v1/schema/**\ *(string: log_name)*

    **Query Parameters:**

    * **log_name** - the name of the log

    Example: ``/v1/schema/ad_click``

    *Example Response* ::

        {
            "log_name": "ad_click",
            "versions": ["initial"]
        }

    ============ ===========
    Status Code  Description
    ============ ===========
    **200**      Success
    **404**      invalid log_name
    **500**      unknown exception
    ============ ===========

    * **Encoding type:** *application/json*
    """
    log_name = request.matchdict.get('log_name', None)

    try:
        if log_name is None:
            return 200, s3_bucket_action(list_all_log_versions)
        else:
            return 200, s3_bucket_action(list_versions_by_log_name, log_name)
    except S3ResponseError as e:
        return e.status, {'error': e.reason}
    except ValueError as e:
        return 404, {'error': e.message}
    except Exception as unknown_exception:
        return 500, {'error': repr(unknown_exception)}
