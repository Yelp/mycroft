#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Rename table inside schema with the provided suffix

Usage:

    cat schema/db.yaml  | sherlock/tools/table_rename.py \
            --suffix '_test_5'
"""


import argparse
import sys
import yaml

from sherlock.common.redshift_schema import RedShiftLogSchema


def create_new_yaml(suffix, input_file=sys.stdin):

    schema = RedShiftLogSchema(yaml.load(input_file))
    for table_name in schema.tables().keys():
        schema.table_rename(
            table_name, '{original_name}{suffix}'.format(
                original_name=table_name,
                suffix=suffix
            )
        )
    return yaml.dump(schema.schema(), default_flow_style=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Rename table with suffix')
    parser.add_argument('--suffix')
    args = parser.parse_args()
    print RedShiftLogSchema.header()
    print create_new_yaml(args.suffix)
