#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A limited functionality script to interact with Redshift databases.
The purpose of this script is to facilitate integration testing.
It is invoked from test_new_schema.sh
"""

import argparse
from yaml import safe_load

from staticconf import YamlConfiguration
from staticconf import read_string

from sherlock.common.aws import get_aws_creds
from sherlock.common.config_util import load_package_config

from sherlock.common.loggers import PipelineStreamLogger
from sherlock.common.redshift_schema import RedShiftLogSchema
from sherlock.common.redshift_cluster_mgmt import RedshiftClusterMgmt
from sherlock.common.util import load_from_file
from sherlock.common.redshift_psql import DEFAULT_NAMESPACE
from sherlock.common.redshift_psql import get_namespaced_tablename
from sherlock.common.redshift_psql import RedshiftPostgres


def rs_cluster_delete(rs_mgmt, args):
    rs_mgmt.delete_cluster(args.cluster_name, not args.skip_snapshot)


def rs_cluster_restore(rs_mgmt, args):
    """ restore cluster from snapshot
    Output can be appended to a YAML config file
    """

    if not args.subnet_group_name:
        args.subnet_group_name = read_string('redshift_cluster_subnet_group_name')
    if not args.vpc_security_group:
        args.vpc_security_group = read_string('security_group_id')
    rs_mgmt.restore_from_cluster_snapshot(
        args.cluster_name,
        args.snapshot,
        args.parameter_group,
        args.vpc_security_group,
        args.subnet_group_name,
    )
    cluster_info = rs_mgmt.get_cluster_info(args.cluster_name)
    return cluster_info['Endpoint']['Address'], cluster_info['Endpoint']['Port']


def rs_show_clusters(rs_mgmt, args):
    """ show available clusters, filter by cmd-line non-regex pattern """
    cluster_list = rs_mgmt.get_cluster_list()
    for cluster in cluster_list:
        if cluster['ClusterIdentifier'].find(args.pattern) >= 0:
            print cluster['ClusterIdentifier']


def rs_check_table_def(psql, db, tables, schemaname):
    """ compare that table definition in Redshift matches the schema """
    query = '''select "column" from pg_table_def where tablename=%(tablename)s \
AND schemaname=%(schemaname)s'''
    for table in tables:
        param_dict = {
            'tablename': table,
            'schemaname': schemaname,
        }
        msg = query % param_dict
        result = psql.run_sql(query, db, msg, params=param_dict, output=True, schema=schemaname)
        expected = set([c.get('name', c.get('log_key')).lower()
                       for c in tables[table]['columns']])
        actual = set([r[0].lower() for r in result])
        if actual != expected:
            raise ValueError("{0} != {1}".format(actual, expected))


def rs_check_table_rows(psql, db, tables, schemaname):
    """ make sure that at least 1 row is available in each table """
    template = "select count(*) from {0}"
    for table in tables:
        namespaced_table = get_namespaced_tablename(table, schemaname=schemaname)
        query = template.format(namespaced_table)
        result = psql.run_sql(query, db, query, output=True)
        if result is False:
            raise ValueError("Error occurred, see {0} \
scribe log".format(psql.log_stream))
        if result[0][0] <= 0:
            raise ValueError("{0}: has zero rows".format(namespaced_table))


def rs_check_schema(rs_mgmt, args):
    yaml_data = load_from_file(args.schema)
    tables = RedShiftLogSchema(safe_load(yaml_data)).tables()

    db = read_string('pipeline.redshift_database')
    log_stream = read_string('pipeline.load_step.s3_to_redshift_stream')
    pipe_strm_lgr = PipelineStreamLogger(
        log_stream,
        True,
        'rs_check_schema'
    )
    psql = RedshiftPostgres(pipe_strm_lgr, args.credentials, run_local=True)
    rs_check_table_def(psql, db, tables, args.redshift_schema)
    rs_check_table_rows(psql, db, tables, args.redshift_schema)


def merge_configs(fname_list):
    base_fname = fname_list.pop(0)
    config = load_package_config(base_fname)
    for fname in fname_list:
        YamlConfiguration(fname, optional=True)
    return config


def set_parser_config_option(parser):
    parser.add_argument(
        "-c", "--config", required=True, action='append',
        help="YAML config files"
    )


def set_cluster_restore_parser(subparsers):
    parser = subparsers.add_parser(
        'cluster_restore',
        description="""create Redshift cluster from snapshot"""
    )
    parser.add_argument(
        "-n", "--cluster-name", required=True,
        help="cluster name"
    )
    parser.add_argument(
        "-s", "--snapshot", required=True,
        help="snapshot name"
    )
    parser.add_argument(
        "--security-group",
        help="cluster security group"
    )
    parser.add_argument(
        "--parameter-group",
        help="cluster parameter group"
    )
    parser.add_argument(
        "--subnet-group-name",
        help="redshift cluster subnet group name",
    )
    parser.add_argument(
        "--vpc-security-group",
        help="redshift vpc security group",
    )
    set_parser_config_option(parser)
    parser.set_defaults(func=rs_cluster_restore)


def set_cluster_delete_parser(subparsers):
    parser = subparsers.add_parser(
        'cluster_delete',
        description="""delete Redshift cluster"""
    )
    parser.add_argument(
        "-n", "--cluster-name", required=True,
        help="cluster name"
    )
    parser.add_argument(
        "-s", "--skip_snapshot", action='store_true',
        help="skip saving cluster snapshot"
    )
    set_parser_config_option(parser)
    parser.set_defaults(func=rs_cluster_delete)


def set_show_clusters_parser(subparsers):
    parser = subparsers.add_parser(
        'show_clusters',
        description="""show all Redshift clusters"""
    )
    parser.add_argument(
        "-p", "--pattern", default='',
        help="filter clusters by pattern (not a REGEX)"
    )
    set_parser_config_option(parser)
    parser.set_defaults(func=rs_show_clusters)


def set_check_schema_parser(subparsers):
    parser = subparsers.add_parser(
        'check_schema',
        description="""check schema against cluster"""
    )
    parser.add_argument(
        "-s", "--schema", required=True,
        help="schema file"
    )
    parser.add_argument(
        "--redshift-schema", default=DEFAULT_NAMESPACE,
        help="redshift schema where tables are stored"
    )
    parser.add_argument(
        "--credentials", required=True,
        help="file with Redshift access credentials"
    )
    set_parser_config_option(parser)
    parser.set_defaults(func=rs_check_schema)


def get_cmd_line_args():
    parser = argparse.ArgumentParser(
        prog='PROG',
        description="""Redshift cluster management""",
    )
    subparsers = parser.add_subparsers()
    set_cluster_restore_parser(subparsers)
    set_cluster_delete_parser(subparsers)
    set_show_clusters_parser(subparsers)
    set_check_schema_parser(subparsers)
    return parser.parse_args()


if __name__ == "__main__":
    args = get_cmd_line_args()
    merge_configs(args.config)
    creds = get_aws_creds(from_local_file=True)
    rs_mgmt = RedshiftClusterMgmt(
        host=read_string('redshift_region_uri'),
        aws_access_key_id=creds.access_key_id,
        aws_secret_access_key=creds.secret_access_key,
        security_token=creds.token,
    )
    args.func(rs_mgmt, args)
