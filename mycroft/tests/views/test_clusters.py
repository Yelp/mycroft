# -*- coding: utf-8 -*-
from mock import patch
import pytest

from mycroft.models.abstract_records import PrimaryKeyError
from mycroft.views.clusters import clusters
from mycroft.views.clusters import cluster_by_name
from tests.views.conftest import dummy_request


@pytest.mark.parametrize("err_instance, expected_return_code", [
    (ValueError(), 404),
    (Exception(), 500)
])
def test_cluster_by_name_errors(err_instance, expected_return_code):
    dr = dummy_request()
    dr.matchdict["log_name"] = "dummy_log_name"

    with patch('mycroft.models.aws_connections.TableConnection.get_connection'):
        with patch('mycroft.views.clusters.list_cluster_by_name') as list_action:
            list_action.side_effect = err_instance
            actual_return_code, _ = cluster_by_name(dr)
        assert actual_return_code == expected_return_code


@pytest.mark.parametrize("err_instance, expected_return_code", [
    (PrimaryKeyError(), 400),
    (ValueError(), 404),
    (ValueError("ConditionalCheckFailedException"), 404),
    (Exception, 500),
])
def test_clusters_errors(err_instance, expected_return_code):
    dr = dummy_request()
    with patch('mycroft.models.aws_connections.TableConnection.get_connection'):
        with patch('mycroft.views.clusters.list_all_clusters') as list_all_clusters:
            list_all_clusters.side_effect = err_instance
            actual_return_code, _ = clusters(dr)
        assert actual_return_code == expected_return_code


@pytest.mark.parametrize("request_method, parm", [
    ("POST", "mycroft.views.clusters.post_cluster"),
    ("GET", "mycroft.views.clusters.list_all_clusters")])
def test_clusters(request_method, parm):
    dr = dummy_request()
    dr.method = request_method
    expected_return_code = 200
    with patch('mycroft.models.aws_connections.TableConnection.get_connection'):
        with patch(parm) as request_method_action:
            request_method_action.return_value = {}
            actual_return_code, _ = clusters(dr)
        assert actual_return_code == expected_return_code


@pytest.mark.parametrize("cluster_name, parm", [
    ("dummy_clustername", "mycroft.views.clusters.list_cluster_by_name"),
    (None, "mycroft.views.clusters.list_cluster_by_name")])
def test_cluster_by_name(cluster_name, parm):
    dr = dummy_request()
    dr.matchdict["cluster_name"] = cluster_name
    expected_return_code = 200
    with patch('mycroft.models.aws_connections.TableConnection.get_connection'):
        with patch(parm) as list_action:
            list_action.return_value = {}
            actual_return_code, _ = cluster_by_name(dr)
        assert actual_return_code == expected_return_code
