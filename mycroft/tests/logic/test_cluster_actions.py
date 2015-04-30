# -*- coding: utf-8 -*-
import pytest
import simplejson
from simplejson import JSONDecodeError

from mycroft.logic.cluster_actions import _parse_clusters
from mycroft.logic.cluster_actions import list_all_clusters
from mycroft.logic.cluster_actions import list_cluster_by_name
from mycroft.logic.cluster_actions import post_cluster

from tests.data.redshift_cluster import REDSHIFT_CLUSTER_INPUT_DICT
from tests.data.redshift_cluster import SAMPLE_REDSHIFT_ID
from tests.data.redshift_cluster import SAMPLE_CLUSTER_ITEMS
from tests.models.test_abstract_records import dynamodb_connection  # noqa
from tests.models.test_redshift_cluster import TestRedshiftClusters as TstRedshiftClusters

BASE_DICT = {
    'redshift_id': None,
    'port': None,
    'host': None,
    'db_schema': None,
    'groups': None,
    'node_type': None,
    'node_count': None,
}


class TestClusterActions(object):

    @pytest.yield_fixture(scope='function')  # noqa
    def redshift_clusters(self, dynamodb_connection):
        rc_object = TstRedshiftClusters()
        table, rc = rc_object._get_redshift_clusters(dynamodb_connection)
        for cluster in SAMPLE_CLUSTER_ITEMS:
            assert rc.put(**cluster)
        yield rc
        assert table.delete()

    def test__parse_clusters_empty_cluster(self):
        empty_clusters = [TstRedshiftClusters().create_fake_redshift_cluster(BASE_DICT)]
        result = _parse_clusters(empty_clusters)
        assert result['clusters'][0] == BASE_DICT

    def test_list_cluster_by_name(self, redshift_clusters):
        return_value = list_cluster_by_name(redshift_clusters, SAMPLE_REDSHIFT_ID)
        assert return_value['redshift_id'] == SAMPLE_REDSHIFT_ID

    @pytest.mark.parametrize("cluster_name", ["..", "x.y"])
    def test_list_cluster_by_name_bad_cluster_name(self, cluster_name):
        with pytest.raises(ValueError) as e:
            list_cluster_by_name(None, cluster_name)
        expected_error = "ValueError: invalid cluster_name: {0}".format(cluster_name)
        assert e.exconly() == expected_error

    def test_list_all_clusters(self, redshift_clusters):
        assert len(list_all_clusters(redshift_clusters)['clusters']) == \
            len(SAMPLE_CLUSTER_ITEMS)

    @pytest.mark.parametrize("cluster_name, port", [
        ("abc", "abc"), ("..", 5439)])
    def test_post_bad_cluster(self, redshift_clusters, cluster_name, port):
        input_dict = dict(REDSHIFT_CLUSTER_INPUT_DICT)
        input_dict["redshift_id"] = cluster_name
        input_dict["port"] = port
        input_string = simplejson.dumps(input_dict)
        with pytest.raises(ValueError):
            post_cluster(redshift_clusters, input_string)

    def test_post_acceptable_cluster(self, redshift_clusters):
        input_string = simplejson.dumps(REDSHIFT_CLUSTER_INPUT_DICT)
        result = post_cluster(redshift_clusters, input_string)
        assert 'post_accepted' in result
        assert result['post_accepted'] is True

    def test_post_duplicate_cluster(self, redshift_clusters):
        input_string = simplejson.dumps(REDSHIFT_CLUSTER_INPUT_DICT)
        result = post_cluster(redshift_clusters, input_string)
        assert 'post_accepted' in result
        assert result['post_accepted'] is True
        with pytest.raises(ValueError):
            post_cluster(redshift_clusters, input_string)

    @pytest.mark.parametrize("missing_key", ['redshift_id', 'port', 'host'])
    def test_post_insufficient_key(self, missing_key):
        test_dict = dict(REDSHIFT_CLUSTER_INPUT_DICT)
        test_dict.pop(missing_key)
        input_string = simplejson.dumps(test_dict)
        with pytest.raises(ValueError) as e:
            post_cluster(None, input_string)
        expected_message = "ValueError: missing the following required args {0}".format(
                           [missing_key])
        assert e.exconly() == expected_message

    def test_post_no_kwargs(self):
        with pytest.raises(JSONDecodeError):
            post_cluster(None, "")
