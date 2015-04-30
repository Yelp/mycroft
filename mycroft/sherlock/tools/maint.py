#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A Redshift database maintenance script
By default, runs analyze on each schema table

Optional operations:
--compact: compact table to reclaim space from deleted rows,
           useful for databases with data retention policies
"""

import argparse
import datetime
from yaml import safe_load

from staticconf import read_string

from sherlock.common.loggers import PipelineStreamLogger
from sherlock.common.redshift_psql import DEFAULT_NAMESPACE
from sherlock.common.redshift_psql import get_namespaced_tablename
from sherlock.common.redshift_psql import RedshiftPostgres
from sherlock.common.redshift_schema import RedShiftLogSchema
from sherlock.common.util import load_from_file
from sherlock.tools.rs_mgmt import merge_configs


def get_cmd_line_args():
    parser = argparse.ArgumentParser(
        prog='PROG',
        description="""Redshift database maintenance""",
    )
    parser.add_argument(
        "-c", "--config", required=True, action='append',
        help="YAML config files"
    )
    parser.add_argument(
        "-s", "--schema", required=True,
        help="schema file"
    )
    parser.add_argument(
        "--credentials", required=True,
        help="file with Redshift access credentials"
    )
    parser.add_argument(
        "--compact", action='store_true',
        help="reclaim free blocks from deleted rows, if any"
    )
    parser.add_argument(
        "-r", "--run-local", action="store_true", default=False,
        help="-r to run on a dev box",
    )
    parser.add_argument(
        "--redshift-schema", default=DEFAULT_NAMESPACE,
        help="redshift schemaname holding the tables"
    )
    return parser.parse_args()


def compact_table(psql, db, tbl_name, run_now=False):
    # run once-a-week on Saturday or if previous
    # attempt did not finish or if run_now is True
    ok_to_run = run_now
    now = datetime.datetime.utcnow()

    if not ok_to_run:
        weekday = now.weekday() + 1
        ok_to_run = (weekday == 6)

    if ok_to_run:
        query = 'vacuum delete only {0}'.format(tbl_name)
        psql.run_sql(query, db, query, need_commit=False)


def compact_tables(psql, db, tables, schemaname=DEFAULT_NAMESPACE):
    for tbl_name in tables:
        tbl_name = get_namespaced_tablename(tbl_name, schemaname)
        compact_table(psql, db, tbl_name, run_now=True)


def analyze_table(psql, db, tbl_name):
    query = 'analyze {0}'.format(tbl_name)
    psql.run_sql(query, db, query)


def analyze_tables(psql, db, tables, schemaname=DEFAULT_NAMESPACE):
    num_failures = 0
    for tbl_name in tables:
        tbl_name = get_namespaced_tablename(tbl_name, schemaname)
        try:
            analyze_table(psql, db, tbl_name)
        except:
            num_failures += 1
    if num_failures:
        raise RuntimeError(
            'failed to analyze {0} tables, see log'.format(num_failures)
        )


if __name__ == "__main__":
    args = get_cmd_line_args()
    run_local = args.run_local
    merge_configs(args.config)
    db = read_string('pipeline.redshift_database')
    log_stream = read_string('pipeline.load_step.s3_to_redshift_stream')
    logstream = PipelineStreamLogger(log_stream, run_local, 'redshift_maint')
    psql = RedshiftPostgres(logstream, args.credentials, run_local=run_local)

    yaml = load_from_file(args.schema)
    schema = RedShiftLogSchema(safe_load(yaml))

    if args.compact:
        compact_tables(psql, db, schema.tables(), args.redshift_schema)
    analyze_tables(psql, db, schema.tables(), args.redshift_schema)
