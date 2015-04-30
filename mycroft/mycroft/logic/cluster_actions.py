# -*- coding: utf-8 -*-
"""
**logic.cluster_actions**
=========================

A collection of functions to handle actions relating to clusters
in the mycroft service.  The C part of MVC for mycroft/clusters
"""

import simplejson
import re

from sherlock.common.redshift_psql import DEFAULT_NAMESPACE

MAX_CLUSTER_NAME_LENGTH = 63
REDSHIFT_ID_RE = re.compile(r"""^[a-zA-Z](-?[a-zA-Z0-9])+$""")
REQUIRED_POST_ARGS = frozenset([
    "redshift_id",
    "port",
    "host",
])

CLUSTER_KWARGS = {
    'redshift_id': None,
    'port': None,
    'host': None,
    'db_schema': None,
    'groups': None,
    'node_type': None,
    'node_count': None,
}


def _parse_clusters(query_result):
    """
    _parse_clusters converts the results of a query on the backing store
    redshift_cluster_object to a dictionary with a key of 'clusters' and value
    of a list of clusters

    *Each cluster returned in the list is of the form*::

        {
            'port': None,
            'host': None,
            'db_schema': None,
            'groups': None,
            'redshift_id': None,
        }


    :param query_result: iterable of RedshiftCluster elements
    :type query_result: iterable

    :returns: A dict of \{'clusters': [cluster1, cluster2, ...]\}
    :rtype: dict

    """
    return {'clusters': [_construct_cluster_dict(cluster) for cluster in query_result]}


def _construct_cluster_dict(cluster_object):
    cluster_dict = cluster_object.get(**CLUSTER_KWARGS)
    groups = cluster_dict.get('groups', None)
    if groups:
        cluster_dict['groups'] = list(groups)
    return cluster_dict


def list_all_clusters(redshift_clusters_object):
    """
    lists all clusters

    *Each cluster returned in the list is of the form*::

        {
            'port': None,
            'host': None,
            'db_schema': None,
            'groups': None,
            'redshift_id': None,
        }

    :param redshift_clusters_object: the RedshiftClusters from which we read clusters
    :type redshift_clusters_object: an instance of RedshiftClusters

    :returns: A dict of \{'clusters': [cluster1, cluster2, ...]\}
    :rtype: dict

    """
    all_clusters = [cluster for cluster in redshift_clusters_object]
    return _parse_clusters(all_clusters)


def list_cluster_by_name(redshift_clusters_object, cluster_name):
    """
    lists all clusters for a particular cluster_name

    *Each cluster returned in the list is of the form*::

        {
            'port': None,
            'host': None,
            'db_schema': None,
            'groups': None,
            'redshift_id': None,
        }

    :param redshift_clusters_object: the RedshiftClusters from which we read clusters
    :type redshift_clusters_object: an instance of RedshiftClusters
    :param cluster_name: name of the redshift cluster (e.g., cluster)
    :type cluster_name: string

    :returns: a dict of {'clusters': [cluster1, cluster2, ...]}
    :rtype: dict
    """
    if cluster_name is None:
        raise ValueError("no cluster name")
    if REDSHIFT_ID_RE.match(cluster_name) is None:
        raise ValueError("invalid cluster_name: {0}".format(cluster_name))
    if len(cluster_name) > MAX_CLUSTER_NAME_LENGTH:
        raise ValueError("invalid cluster_name: {0} exceeds {1} char limit {0}".format(
            cluster_name, MAX_CLUSTER_NAME_LENGTH))

    cluster = redshift_clusters_object.get(redshift_id=cluster_name)
    return _construct_cluster_dict(cluster)


def _check_required_args(param_dict):
    """
    :param param_dict: parameters to enter into the backing store
    :type param_dict: dictionary

    :returns: None
    :rtype: None
    """
    if not REQUIRED_POST_ARGS.issubset(set(param_dict)):
        missing_args = list(REQUIRED_POST_ARGS - set(param_dict))
        raise ValueError("missing the following required args {0}".format(missing_args))
    if REDSHIFT_ID_RE.match(param_dict['redshift_id']) is None:
        raise ValueError("invalid cluster_name: {0}".format(param_dict['redshift_id']))
    if len(param_dict['redshift_id']) > MAX_CLUSTER_NAME_LENGTH:
        raise ValueError("invalid cluster_name: {0} exceeds {1} char limit {0}".format(
            param_dict['redshift_id'], MAX_CLUSTER_NAME_LENGTH))

    try:
        port = int(param_dict['port'])
    except ValueError:
        raise ValueError("invalid port: {0}".format(param_dict['port']))
    if port < 1000 or port > 65535:
        raise ValueError("invalid port: {0}".format(port))


def post_cluster(redshift_clusters_object, request_body_str):
    """
    the request body should be a dictionary (so \*\*request_body are kwargs),
    and it has the following required keys:

    * redshift_id
    * host
    * port

    :param redshift_clusters_object:  an instance of RedshiftClusters
    :type redshift_clusters_object: RedshiftClusters
    :param request_body: the body of the post as stringified dict
    :type request_body: string

    :returns: success
    :rtype: boolean

    :raises S3ResponseError: if the bytes written don't match the length of
        the content
    """
    request_body_dict = simplejson.loads(request_body_str)
    if 'groups' in request_body_dict and request_body_dict['groups'] is not None:
        request_body_dict['groups'] = set(request_body_dict['groups'])
    else:
        request_body_dict['groups'] = set([])

    if 'db_schema' not in request_body_dict:
        request_body_dict['db_schema'] = DEFAULT_NAMESPACE
    else:
        # store lower case name -- use only lower for case insensitivity
        request_body_dict['db_schema'] = request_body_dict['db_schema'].lower()

    _check_required_args(request_body_dict)

    return {'post_accepted': redshift_clusters_object.put(**request_body_dict)}
