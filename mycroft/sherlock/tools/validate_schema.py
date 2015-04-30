#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
inenv -- python sherlock/tools/validate_schema \
        --file schema/db.yaml
'''
import argparse
import yaml

from sherlock.common.schema import Column
from sherlock.common.schema import Table
from sherlock.common.redshift_schema import RedShiftLogSchema
from sherlock.common.util import load_from_file


def create_schema(file_path):
    yaml_data = load_from_file(file_path)
    schema = RedShiftLogSchema(yaml.load(yaml_data))

    name_to_columns = dict((name, [Column.create_from_table(table, column)
                                   for column in table['columns']])
                           for name, table
                           in schema.tables().iteritems())
    for __, columns in name_to_columns.iteritems():
        assert columns
    name_to_table = dict((name,
                          Table.create(table,
                                       columns=name_to_columns[name]))
                         for name, table in schema.tables().iteritems())
    assert name_to_table


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Validate schema.yaml')
    parser.add_argument('--file', dest='file_path')
    args = parser.parse_args()
    create_schema(args.file_path)
