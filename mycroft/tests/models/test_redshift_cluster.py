# -*- coding: utf-8 -*-
from boto.dynamodb2.table import Table
from contextlib import contextmanager
import pytest

from mycroft.models.redshift_clusters import RedshiftClusters
from tests.data.mock_config import MOCK_CONFIG
from tests.data.redshift_cluster import SAMPLE_REDSHIFT_ID
from tests.data.redshift_cluster import SAMPLE_CLUSTER_ITEMS
from tests.models.test_abstract_records import dynamodb_connection  # noqa
from mycroft.models.aws_connections import get_avro_schema
from tests.models.test_abstract_records import NAME_TO_SCHEMA


@contextmanager
def make_temp_redshift_clusters_table(
    connection,
    table_name=MOCK_CONFIG['aws_config']['redshift_clusters']
):
    table = Table.create(
        table_name,
        schema=NAME_TO_SCHEMA['redshift_clusters'],
        connection=connection
    )
    try:
        yield table
    finally:
        assert table.delete()


class FakeRedshiftCluster(object):
    def __init__(self, fake_input_record):
        """
        FakeRedshiftCluster supports a partial interface for RedshiftCluster
        namely, the __init__ and get methods
        """
        self._fake_record = fake_input_record

    def get(self, **kwargs):
        output_dict = dict((key, self._fake_record.get(key, value))
                           for key, value in kwargs.iteritems())
        return output_dict


class TestRedshiftClusters(object):

    @pytest.yield_fixture  # noqa
    def redshift_clusters(self, dynamodb_connection):
        avro_schema = get_avro_schema('mycroft/avro/redshift_cluster.json')
        with make_temp_redshift_clusters_table(dynamodb_connection, 'RedshiftClusters') as table:
            redshift_clusters = RedshiftClusters(
                persistence_object=table,
                avro_schema_object=avro_schema
            )
            for redshift_cluster in SAMPLE_CLUSTER_ITEMS:
                assert redshift_clusters.put(**redshift_cluster)
            yield redshift_clusters

    @classmethod
    def create_fake_redshift_cluster(cls, keyword_dict):
        return FakeRedshiftCluster(keyword_dict)

    def _get_redshift_clusters(self, dynamodb_connection):  # noqa
        """
        WARNING -- this method requires cleanup; the user must remember to
        delete the table once complete.  For example:

        >>> NEW_JOB = {'redshift_id': 'rs_1', 'host': 'host', 'port': 5439}
        >>> def cool_test_fn(dynamodb_connection):
        >>>     trc = TestRedshiftClusters()
        >>>     table, redshift_clusters = trc._get_redshift_clusters(dynamodb_connection)
        >>>     assert redshift_clusters.put(**NEW_JOB)
        >>>     yield redshift_clusters
        >>>     assert table.delete()  # THIS IS THE KEY CLEANUP!!

        """
        avro_schema = get_avro_schema('mycroft/avro/redshift_cluster.json')
        table = Table.create(
            'RedshiftClusters',
            schema=NAME_TO_SCHEMA['redshift_clusters'],
            connection=dynamodb_connection)
        return table, RedshiftClusters(persistence_object=table, avro_schema_object=avro_schema)

    def test_get(self, redshift_clusters):
        # query by a value
        # make sure it includes only the value we are looking for
        cluster = redshift_clusters.get(redshift_id=SAMPLE_REDSHIFT_ID)
        # We have to convert the iterable to list because
        # there are multiple properties we want to verify
        assert cluster.get(redshift_id=None)['redshift_id'] == SAMPLE_REDSHIFT_ID
