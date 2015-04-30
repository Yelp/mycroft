#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A utility to convert a JSON log file into a db configuration
suitable for creating redshift schema

Example:

zcat ~/logstash-test/log4-r4-iad1-00074.gz | \
head -n 1000 | sherlock/tools/log2schema.py -t ranger -d 2
"""

import argparse
import fileinput
import sys
import textwrap

import simplejson
import yaml

from sherlock.common.redshift_schema import RedShiftLogSchema
from redshift_schema_maker import RedShiftSchemaMaker
from sherlock.common.util import get_deep


class Log2SchemaUnsupportedType(Exception):
    pass


def process_row(sm, row, options):
    """ Process row from the log
    sm: RedShiftSchemaMaker object
    row: a dict representing a log line
    options: command line options
    """
    for f in options.foreign:
        foreign_obj = get_deep(row, f)
        if foreign_obj is not None:
            sm.process_foreign_object(f, foreign_obj)

    obj = get_deep(row, options.source)
    if obj is None:
        return

    if options.source_type == 'sparse_list':
        for val in obj.values():
            sm.process_object(val, options.depth)
    elif options.source_type == 'dict':
        sm.process_object(obj, options.depth)
    elif options.source_type == 'list':
        for element in obj:
            sm.process_object(element, options.depth)
    else:
        raise Log2SchemaUnsupportedType(options.source_type, row)


def get_cmd_line_args(
        description="""Analyze json log to create RedShift YAML table""",
        cmd_input="""zcat log.gz | head -n 100 | """):

    defaults = {
        'depth': -1,
        'source': '',
        'source_type': 'dict',
        'foreign': [],
        'table': 'some_name',
        'exclude': [],
        'prune': [],
    }
    parser = argparse.ArgumentParser(
        prog='PROG',
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent('''\

FAQ:
Q: When to use source_type 'list'?
A: If data is an array such as search.results. \
See example below

Q: When to use source_type 'sparse_list'?
A: If data is a dictionary whose keys are numbers such as experiments. \
See example below

Q: What are 'foreign' keys?
A: Foreign keys are external keys that can be used for joining \
tables.
   For example 'request_id' can be a foreign key \
when generating table 'search'
   from source 'search.results'. See example below

Q: How to exclude keys?
A: Exclude keys by specifying path relative to source.
   See example below.

Q: How to prevent deep recursion for certain keys?
A: Use -p <key> option, see example below.

EXAMPLES:
# generate table 'main'
{0}PROG -t main

# generate table 'search' from the nested data
{0}PROG -t search -s extra.search

# exclude 'extra.search.parsed_location' and 'extra.search.index_versions' \
from table 'search'
{0}PROG -t search -s extra.search \
-e parsed_location -e index_versions

# do not recurse farther than 'extra.search.parsed_location'
{0}PROG -t search -s extra.search -p parsed_location

# add foreign keys to table 'search' for ease of joins
{0}PROG -t search -s extra.search \
-f start_time -f location_setting.zip

# handle list data.  Note: 'index' column to store array index
{0}PROG -t results -s search.results\
 --source_type list

# handle dict data where key is a free-form integer.  \
Note: 'index' column to store integer key
{0}PROG -t experiments -s \
session_info.experiments --source_type sparse_list

'''.format(cmd_input))
    )
    parser.set_defaults(**defaults)
    parser.add_argument(
        "-d", "--depth",
        help="[ %(default)s ] object depth to recurse, -1 => all the way",
        type=int
    )
    parser.add_argument(
        "-f", "--foreign",
        help="%(default)s add foreign keys to the table", action='append'
    )
    parser.add_argument(
        "-e", "--exclude",
        help="%(default)s exclude keys from the table", action='append'
    )
    parser.add_argument(
        "-t", "--table",
        help="[ %(default)s ] table name"
    )
    parser.add_argument(
        "-p", "--prune",
        help="%(default)s keys for which stop recursion", action='append'
    )
    parser.add_argument(
        "-s", "--source",
        help="[ %(default)s ] path in dict to start recursion"
    )
    parser.add_argument(
        "--source-type",
        help="[ %(default)s ] treat source as <list|sparse_list|dict>"
    )
    parser.add_argument(
        "--add-source-filename", action='store_true',
        help="[%(default)s] column indicating source of each table row"
    )
    parser.add_argument(
        "--merge-with-schema", metavar='PATH_TO_SCHEMA_FILE',
        help="path to schema file into which new table is merged"
    )
    args = parser.parse_args()
    args.foreign.sort()
    return args


if __name__ == "__main__":
    args = get_cmd_line_args()

    schema = RedShiftLogSchema()
    version = 0

    if args.merge_with_schema:
        with open(args.merge_with_schema, 'r') as yaml_file:
            schema = RedShiftLogSchema(yaml.safe_load(yaml_file))
            if schema.get_table(args.table) is not None:
                schema.table_delete(args.table)
            version = schema.version_get()

    sm = RedShiftSchemaMaker(exclude=args.exclude, prune=args.prune)
    for line in fileinput.input('-'):
        process_row(sm, simplejson.loads(line), args)

    table = sm.mk_table(sm.schema, args.table, args.source, args.source_type,
                        add_source_filename=args.add_source_filename)
    schema.table_add(args.table, table[args.table])
    schema.version_set(version + 1)
    sys.stdout.write(schema.header())
    sys.stdout.write(yaml.dump(schema.schema(), default_flow_style=False))
