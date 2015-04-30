# -*- coding: utf-8 -*-
"""
**views.clusters**
==================

This module contains the clusters function that handles requests for the clusters
endpoint of the mycroft service.
"""
from pyramid.view import view_config

from mycroft.logic.cluster_actions import list_all_clusters
from mycroft.logic.cluster_actions import list_cluster_by_name
from mycroft.logic.cluster_actions import post_cluster

from mycroft.models.aws_connections import TableConnection
from mycroft.models.abstract_records import PrimaryKeyError


@view_config(route_name='api.clusters', request_method='GET', renderer='json_with_status')
@view_config(route_name='api.clusters', request_method='POST', renderer='json_with_status')
def clusters(request):
    """
    clusters handles GET and POST requests from the clusters endpoint

    **GET /v1/clusters/**

    Example: ``/v1/clusters/``

    *Example Response* ::

        [
            {
                'redshift_id': 'cluster-1',
                'port': 5439,
                'host': 'cluster-1.account.region.redshift.amazonaws.com',
                'db_schema': 'public',
                'groups': ['search_infra', 'biz']
            },
            {
                'redshift_id': 'cluster-1-user',
                'port': 5439,
                'host': 'cluster-1-user.account.region.redshift.amazonaws.com',
                'db_schema': 'public',
                'groups': ['search_infra', 'log_infra']
            },
            {
                'redshift_id': 'cluster-2',
                'port': 5439,
                'host': cluster-2.account.region.redshift.amazonaws.com,
                'db_schema': 'public',
                'groups': ['mobile', 'log_infra']
            },
        ]


    ============ ===========
    Status Code  Description
    ============ ===========
    **200**      Success
    **500**      unknown exception
    ============ ===========

    * **Encoding type:** *application/json*

    **POST /v1/clusters/**


    Example: ``/v1/clusters``

    **Query Parameters:**

    * **request.body** -- the json string of cluster details

    *Example request.body* ::

        "{
            'redshift_id': 'cluster-2',
            'port': 5439,
            'host': 'cluster-2.account.region.redshift.amazonaws.com'
        }"

    ============ ===========
    Status Code  Description
    ============ ===========
    **200**      Success
    **404**      invalid cluster parameters
    **500**      unknown exception
    ============ ===========

    * **Encoding type:** *application/json*
    """

    try:
        if request.method == "POST":
            return 200, post_cluster(TableConnection.get_connection('RedshiftClusters'),
                                     request.body)
        elif request.method == "GET":
            return 200, list_all_clusters(TableConnection.get_connection('RedshiftClusters'))
    except PrimaryKeyError as e:
        return 400, {'error': 'bad hash_key or missing required arguments'}
    except ValueError as e:
        if "ConditionalCheckFailedException" in repr(e):
            return 404, {'error': "ConditionalCheckFailed; possible duplicate cluster"}
        return 404, {'error': repr(e)}
    except Exception as unknown_exception:
        return 500, {'error': repr(unknown_exception)}


@view_config(route_name='api.clusters_cluster_name', request_method='GET',
             renderer='json_with_status')
def cluster_by_name(request):
    """
    cluster_by_name returns a dictionary with key "clusters" and value of a
    list of clusters with one entry -- the cluster specified in the route

    **GET /v1/clusters/**\ *{string: cluster_name}*

    **Query Parameters:**

    * **cluster_name** - the name of the log for which we want to see clusters

    Example: ``/v1/clusters/cluster-1``

    *Example Response* ::

        { 'clusters': [
            {
                'redshift_id': 'cluster-1',
                'port': 5439,
                'host': 'cluster-1.account.region.redshift.amazonaws.com',
                'db_schema': 'public',
                'groups': ['search_infra', 'biz']
            }]
        }

    ============ ===========
    Status Code  Description
    ============ ===========
    **200**      Success
    **404**      invalid cluster_name
    **500**      unknown exception
    ============ ===========

    * **Encoding type:** *application/json*
    """
    cluster_name = request.matchdict.get('cluster_name')

    try:
        return 200, list_cluster_by_name(TableConnection.get_connection('RedshiftClusters'),
                                         cluster_name)
    except ValueError as e:
        return 404, {'error': repr(e)}
    except Exception as unknown_exception:
        return 500, {'error': repr(unknown_exception)}
