import mock
import pytest
import staticconf.testing
import sys
from contextlib import contextmanager

import mycroft.batch.add_cluster
import mycroft.models.aws_connections
from mycroft.batch.add_cluster import AddClusterBatch
from mycroft.models.redshift_clusters import RedshiftClusters
from tests.models.test_abstract_records import dynamodb_connection
from tests.models.test_redshift_cluster import make_temp_redshift_clusters_table
from tests.data.mock_config import MOCK_CONFIG


class TestAddCluster(object):
    redshift_id = 'Mycroft'

    @pytest.fixture
    def host(self):
        return 'cluster.us-west-2.redshift.amazonaws.com'

    @pytest.fixture
    def port(self):
        return 5439

    @pytest.fixture
    def expected_cluster_details(self, host, port):
        return dict(
            redshift_id=self.redshift_id,
            host=host,
            port=port,
            db_schema='public'
        )

    @pytest.fixture
    def add_cluster_args(self, host, port):
        return [
            '--redshift-host', host,
            '--redshift-port', str(port),
        ]

    @pytest.fixture
    def other_cluster_args(self, add_cluster_args):
        args = list(add_cluster_args)
        args[1] = "other.{0}".format(args[1])
        return args

    @pytest.fixture
    def add_cluster_idempotent_args(self, add_cluster_args):
        return add_cluster_args + ['--idempotent']

    @pytest.yield_fixture
    def add_batch(self, add_cluster_args, dynamodb_connection):
        with self._setup_batch(add_cluster_args, dynamodb_connection):
            yield AddClusterBatch()

    @pytest.yield_fixture
    def idempotent_add_batch(self, add_cluster_idempotent_args, dynamodb_connection):
        with self._setup_batch(add_cluster_idempotent_args, dynamodb_connection):
            yield AddClusterBatch()

    @pytest.yield_fixture
    def mock_load_default_config(self):
        with mock.patch.object(
            mycroft.batch.add_cluster,
            'load_default_config'
        ) as mock_load_default_config:
            yield mock_load_default_config

    def test_args(self, add_batch, host, port):
        assert add_batch.args.redshift_host == host
        assert add_batch.args.redshift_port == port

    def test_adding_cluster(self, add_batch, mock_load_default_config, expected_cluster_details):
        self._assert_cluster_does_not_exist()
        add_batch.run()
        mock_load_default_config.assert_called_once_with(
            '/mycroft/config.yaml',
            '/mycroft_config/config-env-docker.yaml'
        )
        self._assert_cluster_exists(expected_cluster_details)

        with pytest.raises(SystemExit):
            add_batch.run()

        self._assert_cluster_exists(expected_cluster_details)

    def test_idempotent_adding_cluster(self, idempotent_add_batch, mock_load_default_config, expected_cluster_details):
        self._assert_cluster_does_not_exist()
        idempotent_add_batch.run()
        mock_load_default_config.assert_called_once_with(
            '/mycroft/config.yaml',
            '/mycroft_config/config-env-docker.yaml'
        )
        self._assert_cluster_exists(expected_cluster_details)
        idempotent_add_batch.run()
        self._assert_cluster_exists(expected_cluster_details)

    @contextmanager
    def _setup_batch(self, args, connection):
        with staticconf.testing.MockConfiguration(MOCK_CONFIG):
            with mock.patch.object(mycroft.models.aws_connections, 'get_dynamodb_connection') as mock_conn:
                mock_conn.return_value = connection
                with make_temp_redshift_clusters_table(connection):
                    with mock.patch.object(sys, 'argv', new=['create_cluster.py'] + args):
                        yield

    def _assert_cluster_exists(self, expected_cluster_details):
        conn = self._get_connection()
        cluster = conn.get(redshift_id=self.redshift_id)
        actual = cluster.get(**dict((k, None) for k in ['host', 'port', 'db_schema', 'redshift_id']))
        assert actual == expected_cluster_details

    def _assert_cluster_does_not_exist(self):
        conn = self._get_connection()
        with pytest.raises(KeyError):
            conn.get(redshift_id=self.redshift_id)

    def _get_connection(self):
        return mycroft.models.aws_connections.TableConnection.get_connection('RedshiftClusters')
