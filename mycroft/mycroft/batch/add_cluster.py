#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This batch can be used to add clusers to Mycroft.  If the --idempotent
option is used, the cluster will be upserted.

Recommended Usage:
    docker run -i -t mycroft python -m mycroft.batch.add_cluster --help
"""
from mycroft.models.aws_connections import TableConnection
from sherlock.common.config_util import load_default_config

import argparse
import os


class AddClusterBatch(object):
    @property
    def args(self):
        if not hasattr(self, '_args'):
            parser = argparse.ArgumentParser(description='Add a redshift cluster.')
            parser.add_argument(
                '--mycroft-config',
                type=str,
                help="Mycroft config file (default: %(default)s)",
                default=os.environ.get(
                    'SERVICE_CONFIG_PATH',
                    '/mycroft/config.yaml'
                )
            )
            parser.add_argument(
                '--mycroft-env-config',
                type=str,
                help="Mycroft environment config file (default: %(default)s)",
                default=os.environ.get(
                    'SERVICE_ENV_CONFIG_PATH',
                    '/mycroft_config/config-env-docker.yaml'
                )
            )
            parser.add_argument(
                '--cluster-name',
                type=str,
                help="Cluster display name (default: %(default)s)",
                default='Mycroft'
            )
            parser.add_argument(
                '--redshift-host',
                type=str,
                help="Redshift Host",
                required=True
            )
            parser.add_argument(
                '--redshift-port',
                type=int,
                help="Redshift Port",
                required=True
            )
            parser.add_argument(
                '--redshift-schema',
                type=str,
                help="Redshift Schema to load data into (default: %(default)s)",
                default='public'
            )
            parser.add_argument(
                "--idempotent",
                help="Updates an existing cluster if it exists, or add a new one.",
                action='store_true'
            )
            self._args = parser.parse_args()
        return self._args

    @property
    def _table_conn(self):
        if not hasattr(self, '_cached_table_conn'):
            self._configure_mycroft()
            self._cached_table_conn = TableConnection.get_connection('RedshiftClusters')
        return self._cached_table_conn

    @property
    def _redshift_cluster_fields(self):
        return dict(
            redshift_id=self.args.cluster_name,
            host=self.args.redshift_host,
            port=self.args.redshift_port,
            db_schema=self.args.redshift_schema
        )

    def __init__(self):
        pass

    def run(self):
        if self.args.idempotent:
            self._upsert_cluster()
        else:
            self._add_cluster()
        self._display_cluster_info()

    def _configure_mycroft(self):
        load_default_config(
            self.args.mycroft_config,
            self.args.mycroft_env_config
        )

    def _add_cluster(self):
        try:
            self._table_conn.put(
                **self._redshift_cluster_fields
            )
        except ValueError:
            print (
                "The cluster couldn't be added because a cluster with the same "
                "name already exists."
            )
            exit(1)

    def _update_cluster(self):
        cluster = self._get_cluster()
        cluster.update(
            **self._redshift_cluster_fields
        )

    def _upsert_cluster(self):
        try:
            self._update_cluster()
        except KeyError:
            # A KeyError is raised if the cluster isn't found
            self._add_cluster()

    def _get_cluster(self):
        return self._table_conn.get(redshift_id=self.args.cluster_name)

    def _display_cluster_info(self):
        cluster = self._get_cluster()
        print "Added Cluster to Mycroft: {0}".format(cluster.get(
            **dict((k, None) for k in self._redshift_cluster_fields.keys())
        ))


if __name__ == "__main__":
    AddClusterBatch().run()
