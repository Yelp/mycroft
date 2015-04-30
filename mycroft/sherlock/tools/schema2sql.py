#!/usr/bin/python
# -*- coding: utf-8 -*-

""" Convert schema to sql create table command

"""
import sys

from yaml import safe_load
from sherlock.common.redshift_schema import RedShiftLogSchema


class InvalidAttribute(Exception):
    pass


class NameConflict(Exception):
    pass


def mk_create_table_sql_cmd(name, table):
    sql_str = ""
    column_sql = []
    names = set()
    for c in table['columns']:
        col_name = c['name'] if c.get('name') else c['log_key']
        if 'UNKNOWN' in c['sql_attr']:
            raise InvalidAttribute(
                "key: '{0}', sql_attr: '{1}'".format(
                    c['log_key'], c['sql_attr']
                )
            )
        if col_name in names:
            raise NameConflict(name, col_name)
        names.add(col_name)
        column_sql.append(' "{0}" {1}'.format(col_name, c['sql_attr']))
    sql_str += "CREATE TABLE {0}(\n".format(name)
    sql_str += ",\n".join(column_sql) + "\n)"
    if len(table['sortkey_attr']) > 0:
        sortkey_str = " SORTKEY("
        sortkey_str += ", ".join([col for col in table['sortkey_attr']])
        sortkey_str += ")"
        sql_str += sortkey_str
    sql_str += ";\n"
    return sql_str


def tables_to_sql(tables):
    sql_str = ""
    for name in sorted(tables.keys()):
        sql_str += mk_create_table_sql_cmd(name, tables[name])
    return sql_str


def main():
    schema = RedShiftLogSchema(safe_load(sys.stdin))
    sql_str = tables_to_sql(schema.tables())
    sys.stdout.write(sql_str)


if __name__ == "__main__":
    main()
