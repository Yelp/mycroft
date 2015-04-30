#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
This is a tool to maniuplate Redshift schema without manually editing the yaml
defintion. The program takes its input from standard input.

Example Usage:

    cat schema/db.yaml | mk_log_schema.py table_create -s '' --source-type DICT

'''

import argparse
import sys
import yaml
from sherlock.common.redshift_schema import RedShiftLogSchema

valid_source_types = set(['dict', 'list', 'sparse_list'])


def schema_table_create(schema, args):
    schema.table_create(args.table, args.source, args.source_type,
                        args.add_source_filename)


def schema_table_delete(schema, args):
    schema.table_delete(args.table)


def schema_column_add(schema, args):
    schema.column_add(args.table, args.name, args.sql_attr,
                      getattr(args, 'log_key', None), args.json, args.foreign,
                      args.is_noop)


def schema_column_remove(schema, args):
    schema.column_remove(args.table, args.name)


def schema_sortkeys_add(schema, args):
    schema.sortkeys_add(args.table, args.sortkeys)


def set_table_create_parser(subparsers):
    parser = subparsers.add_parser(
        'table_create',
        description="""create table"""
    )
    defaults = {
        'source': '',
        'source_type': 'dict',
        'add_source_filename': False,
    }
    parser.set_defaults(**defaults)
    parser.set_defaults(func=schema_table_create)
    parser.add_argument(
        "-t", "--table", required=True,
        help="table name"
    )
    parser.add_argument(
        "-s", "--source",
        help="[%(default)s] path.to.log.field as a root"
    )
    parser.add_argument(
        "--source-type", choices=sorted(valid_source_types),
        help="[%(default)s] data type of 'source'"
    )
    parser.add_argument(
        "--add-source-filename", action='store_true',
        help="[%(default)s] column indicating source of each table row"
    )


def set_table_delete_parser(subparsers):
    parser = subparsers.add_parser(
        'table_delete',
        description="""delete table""",
    )
    parser.add_argument(
        "-t", "--table", required=True,
        help="table name"
    )
    parser.set_defaults(func=schema_table_delete)


def set_column_add_parser(subparsers):
    parser = subparsers.add_parser(
        'column_add',
        description="""add column to table""",
    )
    parser.add_argument(
        "-t", "--table", required=True,
        help="table name"
    )
    parser.add_argument(
        "-n", "--name", required=True,
        help="column name"
    )
    parser.add_argument(
        "--sql-attr", required=True,
        help="column SQL attributes"
    )
    parser.add_argument(
        "-k", "--log_key",
        help="log key path is relative to 'source' unless\n\
--is_foreign is set in which case path is absolute\n\
if omitted then it is assumed to be a derived field"
    )
    parser.add_argument(
        "--json", action='store_true',
        help="store column as JSON blob"
    )
    parser.add_argument(
        "--foreign", action='store_true',
        help="treat 'log_key' as absolute path"
    )
    parser.add_argument(
        "--is-noop", action='store_true',
        help="Do not extract or compute value for this column"
    )
    parser.set_defaults(func=schema_column_add)


def set_sortkeys_add_parser(subparsers):
    parser = subparsers.add_parser(
        'sortkeys_add',
        description="""add space separated sortkeys"""
    )
    parser.add_argument(
        "-t", "--table", required=True,
        help="table name"
    )
    parser.add_argument(
        "-s", "--sortkeys", required=True,
        nargs='+',
        help="space separted sortkeys"
    )
    parser.set_defaults(func=schema_sortkeys_add)


def set_column_remove_parser(subparsers):
    parser = subparsers.add_parser(
        'column_remove',
        description="""remove column from table""",
    )
    parser.add_argument(
        "-t", "--table", required=True,
        help="table name"
    )
    parser.add_argument(
        "-n", "--name", required=True,
        help="column name"
    )
    parser.set_defaults(func=schema_column_remove)


def get_cmd_line_args():
    parser = argparse.ArgumentParser(
        description="""
This is a tool to maniuplate Redshift schema without manually editing the yaml
defintion. The program takes its input from standard input.

Example Usage:

    cat schema/db.yaml | %(prog)s table_create -s '' --source-type DICT
        """,
        formatter_class=argparse.RawTextHelpFormatter,
    )

    subparsers = parser.add_subparsers()
    set_table_create_parser(subparsers)
    set_table_delete_parser(subparsers)
    set_column_add_parser(subparsers)
    set_column_remove_parser(subparsers)
    set_sortkeys_add_parser(subparsers)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = get_cmd_line_args()
    schema = RedShiftLogSchema(yaml.load(sys.stdin.read()) or dict())
    version = schema.version_get()
    args.func(schema, args)
    schema.version_set(version + 1)
    sys.stdout.write(RedShiftLogSchema.header())
    sys.stdout.write(yaml.dump(schema.schema(), default_flow_style=False))
