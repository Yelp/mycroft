#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
s3_to_redshift.py is uses the RedshiftPostgres class (see redshift_psql.py)
to copy appropriately formatted data from s3 into a table in redshift.  Note
LOAD is kept within the Python file, and create table is read from the
schema/db.sql file.
"""

import copy
import os
import re
import sys
import string
import time
import traceback
from datetime import datetime
from datetime import timedelta
from os.path import join
from yaml import safe_load

from staticconf import read_int
from staticconf import read_string
from staticconf import YamlConfiguration

from sherlock.common.redshift_psql import get_namespaced_tablename
from sherlock.common.redshift_psql import get_redshift_schema
from sherlock.common.redshift_psql import RedshiftPostgres
from sherlock.common.redshift_status import RedshiftStatusTable
from sherlock.common.dynamodb_status import DynamoDbStatusTable
from sherlock.common.pipeline import add_load_args
from sherlock.common.pipeline import get_base_parser
from sherlock.common.pipeline import get_formatted_date
from sherlock.common.pipeline import get_yaml_table_versions
from sherlock.common.pipeline import MAX_TTL_DAYS
from sherlock.common.pipeline import pipeline_yaml_schema_file_path
from sherlock.common.loggers import PipelineStreamLogger
from sherlock.common.redshift_schema import RedShiftLogSchema
from sherlock.common.util import load_from_file
from sherlock.tools.maint import compact_table
from sherlock.tools.schema2sql import tables_to_sql
from sherlock.tools.schema2sql import mk_create_table_sql_cmd
from sherlock.common.config_util import load_package_config

ERROR_MSG_COL_SIZE = 256

#
# queries requiring the redshift search_path to include the schema
# note: we have to set this prior to running these queries otherwise
# the non-default schemas won't show up in pg_table_def.  The setup
# is done automatically in the run_psql query
#
QUERY_GET_SORT_COLUMN = '''SELECT "column", type FROM pg_table_def \
WHERE tablename = %(tablename)s AND sortkey = 1 \
AND schemaname = %(schemaname)s'''

QUERY_TABLE_DEF = '''SELECT SchemaName, TableName, "Column", Type, Encoding, \
DistKey, SortKey, "NotNull" from pg_table_def where tablename=%(tablename)s \
AND schemaname=%(schemaname)s'''

#
# queries requiring namespaced tablenames
#
# LOAD gets a table_name, s3_input_filename, aws_key, and aws_secret
#
LOAD = """\
copy %s from '%s/part' \
CREDENTIALS \
'aws_access_key_id=%%s;aws_secret_access_key=%%s;token=%%s' \
delimiter '%s' \
ESCAPE gzip TRUNCATECOLUMNS TIMEFORMAT as 'epochmillisecs' \
NULL AS '\\0' STATUPDATE ON;"""

QUERY_GET_MIN_MAX_DATE = """SELECT min({1}), max({1}) from {0}"""

QUERY_DELETE_ROWS_BY_DATE = """DELETE from {0} \
where {1} < %(new_min_date)s"""

QUERY_ADD_COLUMN = """alter table {0} \
add column {1} {2} encode {3} {4} default {5}"""


def get_create_commands(input_file, add_error_table=True):
    """
    get_create_command takes an input file and reads the create table sql
    command from it.

    Args:
    input_file -- the full path of the input file
    add_error_table -- boolean flag to add error table to schema

    Returns:
    a list of (command, table_name) tuples where the command is a SQL command
    for creating the table, and the table_name is the name of the table to be
    created.  Important because we don't want to create a table that already
    exists
    """

    # in regex \S is all non-whitespace and \s is whitespace only
    table_regex = re.compile(r'[\s]*(?P<tablename>[\S]+[\s]*)\(')
    command = load_from_file(input_file)

    if input_file[-5:] == ".yaml":
        rs_log_schema = RedShiftLogSchema(safe_load(command))
        if add_error_table:
            err_tbl_name, err_tbl = rs_log_schema.get_error_table()
            rs_log_schema.table_add(err_tbl_name, err_tbl)
        command = tables_to_sql(rs_log_schema.tables())

    commands = command.split('CREATE TABLE')
    table_create_tuples = []
    for cmd in commands[1:]:
        match = table_regex.search(cmd)
        if match is None:
            table_name = None
        else:
            table_name = match.group('tablename')
            table_to_create = get_namespaced_tablename(table_name)
            cmd = cmd.replace(table_name, table_to_create, 1)
        table_create_tuples.append((table_name, "create table " + cmd))
    return table_create_tuples


def get_current_tables(rs_psql, database):
    """
    get_current_tables gets a list of current tables

    Args:
    rs_psql -- the RedshiftPostgres object used to run the SQL command
    database -- the redshift db name to which we copy the table

    Returns:
    a list of table names
    """
    schemaname = get_redshift_schema()

    SELECT = "SELECT tablename \
FROM pg_catalog.pg_tables \
where schemaname=%(schemaname)s"

    params = {'schemaname': schemaname}

    tables = rs_psql.run_sql(
        SELECT,
        database,
        'getting existing tables',
        output=True,
        params=params
    )

    return [tbl_name for (tbl_name,) in tables]


def handle_error(error_msg, logstream):
    """
    handle_error handles writes error messages to the log, and
    raise Exception.

    Args:
    error_msg -- the error to write
    """
    logstream.write_msg('error', error_msg=error_msg)
    raise Exception(error_msg)


def get_table_creates(schema_file, logstream):
    """
    get_table_creates checks that the schema file exists then calls
    get_create_commands to get a list of tuples (see Returns).

    Args:
    schema_file -- the name of the schema file with the create table command

    Returns:
    create_tuples -- a list of (table_name, create_command) tuples, one for
        each tuple to be created
    """

    create_commands = get_create_commands(schema_file)
    for _, table_name in create_commands:
        if not table_name:
            handle_error("no table name in file: {0}".format(
                schema_file), logstream
            )
    return create_commands


def dates_from_rs_status(status_helper, db, logstream,
                         retry_on_err, single_date=None):
    """
    date_from_rs_status gets the jobs that have completed the et step, but
    have not started the load step, and have no jobs before them running or
    in error

    Args:
    status_helper -- a wrapper around a backing store to aid in CRUD
    db -- is the database we query
    logstream -- a PipelineStreamLogger
    retry_on_err -- a boolean, True if we're retrying on errors
    single_date -- date string of the form YYYY-MM-DD if we're \
        only looking for one

    Returns:
    a list of dates to catch up on formatted as strings YYYY/MM/DD
    """
    versions = get_yaml_table_versions(pipeline_yaml_schema_file_path())

    if single_date is not None:
        data_date = get_formatted_date(single_date)
        if data_date is None:
            handle_error("bad input date: {0}".format(single_date), logstream)
        start_datetime = datetime.strptime(data_date, "%Y/%m/%d")
        status_tuples = \
            status_helper.query_et_complete_job(db, versions, data_date)
    else:
        days_back = read_int('pipeline.load_step.days_to_check') + 1
        start_datetime = datetime.utcnow() - timedelta(days=days_back)
        status_tuples = \
            status_helper.query_et_complete_jobs(db, versions, start_datetime)

    if status_tuples is False:
        handle_error(
            "query for complete et job failed, version={0}, date={1}".format(
                versions,
                data_date if single_date is not None else start_datetime
            ),
            logstream
        )

    candidates = []
    last_date = (start_datetime - timedelta(days=1)).strftime("%Y/%m/%d")
    for ddate, ld_status in status_tuples:
        if not one_day_greater(ddate, last_date):
            break
        elif ld_status is None or (ld_status == 'error' and retry_on_err):
            candidates.append(ddate)
        elif ld_status == 'error':
            break
        last_date = ddate
    candidate_string = "candidates dates for load: {0}".format(candidates)
    logstream.write_msg(status='running', extra_msg=candidate_string)
    return candidates


def one_day_greater(recent_date, past_date):
    """
    simple function finds out whether two dates differ by 1 day

    Args:
    recent_date -- the more recent of the two dates in string format YYYY/MM/DD
    past_date -- the older of the two dates in format string YYYY/MM/DD

    Return:
    True / False -- whether the recent date is one day greater than the old;
    """
    rd = datetime.strptime(str(recent_date), "%Y/%m/%d")
    pd = datetime.strptime(str(past_date), "%Y/%m/%d")
    return (rd - pd == timedelta(days=1))


def get_timestamp_column_name(psql, db_name, table):
    """
    auto detect timestamp column name that is also a sort key.

    Args:
    psql -- handle to talk to redshift
    db -- redshift database containing table
    table -- table name

    Return: column name
    Throw: ValueError
    """
    rs_schema = get_redshift_schema()
    params = {
        'tablename': table,
        'schemaname': rs_schema,
    }
    result = psql.run_sql(
        QUERY_GET_SORT_COLUMN,
        db_name,
        "find sort column",
        params=params,
        output=True,
        schema=rs_schema
    )
    column = get_sortkey_column(result, 'timestamp')
    if len(column) == 0:
        column = get_sortkey_column(result, 'date')
    if len(column) == 0:
        return None
    if len(column) > 1:
        raise ValueError("too many sort columns in {0}".format(table))
    return column.pop()


def get_sortkey_column(query_result, expected_sortkey):
    column = []
    for col, sql_type in query_result:
        if sql_type.lower().find(expected_sortkey) >= 0:
            column.append(col)
    return column


def get_min_max_date(psql, db_name, table, column):
    """
    Determine oldest, freshest data in a table

    Args:
    psql -- handle to talk to redshift
    db -- redshift database containing table
    table -- table name
    column -- timestamp column name found via get_timestamp_column_name

    Return: min_date, max_date
    """
    query = QUERY_GET_MIN_MAX_DATE.format(
        get_namespaced_tablename(table),
        column
    )
    result = psql.run_sql(query, db_name, "get min,max date", output=True)
    return result[0][0], result[0][1]


def delete_old_data(psql, db_name, table, ttl_days):
    """
    Delete data older than TTL.  Round-down min_date, max_date to 00:00:00 UTC
    for cutoff calculation.

    Args:
    psql -- handle to talk to redshift
    db -- redshift database containing table
    table -- table name
    ttl_days -- max TTL of data in a table

    Return: None
    """
    cname = get_timestamp_column_name(psql, db_name, table)
    if cname is None:
        return 0
    dt_min_date, dt_max_date = get_min_max_date(psql, db_name, table, cname)
    if dt_min_date is None or dt_max_date is None:
        return 0

    # cutoff is always YYYY-MM-DD 00:00:00
    dt_min = datetime(dt_min_date.year, dt_min_date.month, dt_min_date.day)
    dt_max = datetime(dt_max_date.year, dt_max_date.month, dt_max_date.day)

    num_days = (dt_max - dt_min).days
    num_deleted = 0
    if ttl_days is not None and num_days > ttl_days:
        dt_new_min_date = dt_min + timedelta(days=num_days - ttl_days)
        new_min_date = datetime.strftime(dt_new_min_date, "%Y-%m-%d %H:%M:%S")
        query = QUERY_DELETE_ROWS_BY_DATE.format(
            get_namespaced_tablename(table),
            cname
        )
        params = {'new_min_date': new_min_date}
        result = psql.run_sql_ex(query, db_name, "delete rows", params=params)
        if result is not False:
            match = re.search(r"^DELETE\s+(?P<num_deleted>\d+)$",
                              result.get('status', ''))
            num_deleted = int(match.group('num_deleted')) if match else 0
        if num_deleted <= 0:
            raise ValueError("nothing to delete for {0}".format(table))
    return num_deleted


class PgTableDef(object):
    SchemaName, TableName, Column, Type, \
        Encoding, DistKey, SortKey, NotNull = range(8)


def get_table_def(psql, db, tablename):
    """ Retrieve table definition stored in the database.
    Table definitions are in pg_table_def, see QUERY_TABLE_DEF for details

    tablename -- table name for which to get definition

    Returns: A list of tuples.  Each entry in the list is for a table column,
    Each tuple describes column attributes such as name, encoding, etc.
    NOTE: For tables not in the database, get_table_def returns empty list
    """
    rs_schema = get_redshift_schema()
    param_dict = {
        'tablename': tablename,
        'schemaname': rs_schema,
    }
    results = psql.run_sql(
        QUERY_TABLE_DEF,
        db,
        "getting table def",
        params=param_dict,
        output=True,
        schema=rs_schema
    )
    return results


def has_table_def(table_def):
    """ Check if table is defined in the database.
    """
    return len(table_def)


def compare_table_defs(psql, db, table, cur_tbl_def, tmp_tbl_def):
    """
    Compare table definitions before allowing supported modifications.
    Currently, only adding new columns is allowed.

    Args:
    psql -- handle to talk to redshift
    db -- redshift database containing table
    table -- table name for which definition may change
    cur_tbl_def -- table definition for existing table
    tmp_tbl_def -- table definition for temp table which may contain changes

    Return: None
    """
    copy_tmp_tbl_def = copy.deepcopy(tmp_tbl_def)

    if not has_table_def(cur_tbl_def):
        raise ValueError("missing existing table: {0}".format(table))

    if len(tmp_tbl_def) < len(cur_tbl_def):
        raise ValueError("{0}: new schema has less columns".format(table))

    for row in cur_tbl_def:
        tmp_row = copy_tmp_tbl_def.pop(0)
        diff = [i for i in range(len(row)) if row[i] != tmp_row[i]
                and i not in [PgTableDef.TableName, PgTableDef.Encoding]]
        if diff:
            raise ValueError("{0}: change to column '{1}' not allowed".format(
                table, row[PgTableDef.Column]
            ))


def add_columns(psql, db, ddate, table, to_add,
                tbl_tuple, defaults, logstream):
    """
    Add new columns to existing table.  Copy data into temp table
    to detect encoding.

    Args:
    psql -- handle to talk to redshift
    db -- redshift database containing table
    ddate -- the date string of the data to be copied formatted YYYY/MM/DD
    table -- table name where to add columns -- not namespaced
    to_add -- list of columns to add
    tbl_tuple -- a tuple containing path to table in PSV format and a
                 Redshift temporary table where to load data
    logstream -- a PipelineStreamLogger

    Return: None
    """
    _, tmp_tbl_name = tbl_tuple
    for row in to_add:
        if row[PgTableDef.SortKey] or row[PgTableDef.DistKey]:
            raise ValueError("{0}: {1} new column is a sortkey \
or distkey".format(table, row[PgTableDef.Column]))

    if to_add:
        # copy data into tmp_tbl_name in order to detect encoding
        copy_table(psql, db, ddate, tbl_tuple, MAX_TTL_DAYS, logstream)
        tmp_tbl_def = get_table_def(psql, db, tmp_tbl_name)
        for row in tmp_tbl_def[len(tmp_tbl_def) - len(to_add):]:
            encoding = row[PgTableDef.Encoding]
            query = QUERY_ADD_COLUMN.format(
                get_namespaced_tablename(table),
                row[PgTableDef.Column],
                row[PgTableDef.Type],
                "raw" if encoding == "none" else encoding,
                "not null" if row[PgTableDef.NotNull] else "null",
                defaults[row[PgTableDef.Column]]
            )
            psql.run_sql(query, db, query)


def get_column_defaults(table):
    """ Extract column default value for each column.
        If column lacks default, set default to NULL.

        Returns: a map(column_name, default_value)
    """
    defaults = dict()
    regex1 = re.compile(r'default\s+(\(|"|\')(.*)(?=(\)|"|\'))')
    regex2 = re.compile(r'default\s+(?P<default>[\S]*)')
    for col in table['columns']:
        col_name = col.get('name', col.get('log_key'))
        defaults[col_name] = 'NULL'
        match1 = regex1.search(col['sql_attr'])
        match2 = regex2.search(col['sql_attr'])
        if match1:
            defaults[col_name] = ''.join(match1.groups())
        elif match2:
            defaults[col_name] = match2.group('default')
    return defaults


def create_tables(psql, db, create_tuples):
    """ Create tables if not
    create_tuples -- a list of (table, sql table create command)
    """
    # create tables if missing for schema
    current_tables = get_current_tables(psql, db)
    for table, create in create_tuples:
        if table not in current_tables:
            psql.run_sql(create, db, " creating table: {0}".format(table))


def update_database_schema(psql, db, ddate, s3_logdir, schema_file, logstream):
    """
    Check new schema against what exists in the database such as
        1.  create new tables if missing
        2.  compare table definitions
        3.  add new columns

    Args:
    psql -- handle to talk to redshift
    db -- redshift database containing table
    ddate -- the date string of the data to be copied formatted YYYY/MM/DD
    s3_logdir -- path to location of tables in PSV format
    schema_file -- the name of the schema file with the create table command
    logstream -- a PipelineStreamLogger

    Return: None
    """
    # TODO: db.yaml as SPOT
    fname = schema_file.replace('.sql', '.yaml')
    yaml_dict = load_from_file(fname)
    rs_log_schema = RedShiftLogSchema(safe_load(yaml_dict))
    err_tbl_name, err_tbl = rs_log_schema.get_error_table()
    rs_log_schema.table_add(err_tbl_name, err_tbl)
    tables = rs_log_schema.tables()

    # create tables if missing for schema
    create_tuples = get_table_creates(schema_file, logstream)
    create_tables(psql, db, create_tuples)

    # check for schema changes
    for table in tables.keys():
        tmp_tbl_name = "tmp_{0}".format(table)
        namespaced_tmp_table = get_namespaced_tablename(tmp_tbl_name)

        # create temp tables
        create_table_cmd = mk_create_table_sql_cmd(namespaced_tmp_table, tables[table])
        psql.run_sql(create_table_cmd, db, create_table_cmd)

        try:
            # fetch table definition
            cur_tbl_def = get_table_def(psql, db, table)
            tmp_tbl_def = get_table_def(psql, db, tmp_tbl_name)
            compare_table_defs(psql, db, table, cur_tbl_def, tmp_tbl_def)

            tbl_tuple = (join(s3_logdir, ddate, table), tmp_tbl_name)
            to_add = tmp_tbl_def[len(cur_tbl_def):]
            defaults = get_column_defaults(tables[table])
            add_columns(psql, db, ddate, table, to_add,
                        tbl_tuple, defaults, logstream)
        finally:
            if tmp_tbl_name != table:
                delete_table_cmd = 'drop table {0}'.format(namespaced_tmp_table)
                psql.run_sql(delete_table_cmd, db, delete_table_cmd)


def copy_table(psql_helper, db_name, ddate, log_tuple, ttl_days, logstream):
    s3_log, rs_table = log_tuple
    namespaced_table_name = get_namespaced_tablename(rs_table)
    table_start = time.time()
    extra_msg = "from s3 log: {0}".format(s3_log)
    logstream.write_msg('starting', extra_msg=extra_msg)

    # about to load new day, remove oldest
    rows_deleted = None
    if ttl_days is not None:
        rows_deleted = \
            delete_old_data(psql_helper, db_name, rs_table, ttl_days - 1)
    if rows_deleted:
        logstream.write_msg('delete_ok',
                            extra_msg="{0} rows".format(rows_deleted))

    # Try to reclaim disk space.  If not needed, it will be fast.
    # Calling here and not in the 'if rows_deleted' code to prevent
    # scenario where rows were deleted but compact failed. Then on retry
    # there will be nothing to delete but since space is not reclaimed
    # there may not be enough for a new load, resulting in failure forever.
    if ttl_days is not None:
        compact_table(psql_helper, db_name, namespaced_table_name)

    delimiter = read_string('redshift_column_delimiter')
    delimiter = delimiter.decode("string_escape")
    if delimiter not in string.printable:
        delimiter = '\\' + oct(ord(delimiter))

    copy_sql = LOAD % (namespaced_table_name, s3_log, delimiter)
    result = psql_helper.run_sql(
        copy_sql,
        db_name, " copying from " + s3_log,
        s3_needed=True,
        time_est_secs=read_int('pipeline.load_step.copy_time_est_secs')
    )
    if result is not False:
        logstream.write_msg('complete', job_start_secs=table_start,
                            extra_msg=extra_msg)
    return result


def copy_tables(psql_helper, status_helper,
                db_name, ddate, log_tuples, ttl_days, logstream):
    """
    copy_tables takes a list of input log, table pairs and copies each
    input log to its corresponding input table

    Args:
    psql_helper -- a RedshiftPostgres object to help perform the copy
    status_helper -- An object handle to interact with status table
    db_name -- the name of the db to which we're copying
    ddate -- the date string of the data to be copied formatted YYYY/MM/DD
    log_tuples -- a list of (log, table) pairs
    ttl_days -- how many days to retain loaded data
    logstream -- a PipelineStreamLogger

    Returns:
    ---
    """
    start = time.time()
    yaml_versions = get_yaml_table_versions(pipeline_yaml_schema_file_path())
    status_helper.update_status(db_name, ddate, yaml_versions, "running")
    err_tbl_name, _ = RedShiftLogSchema().get_error_table()
    for log_tuple in log_tuples:
        result = False
        error_msg = None
        try:
            result = copy_table(psql_helper, db_name, ddate,
                                log_tuple, ttl_days, logstream)
        except KeyboardInterrupt:
            result = None
            raise
        except Exception:
            exc_type, exc_value, exc_tb = sys.exc_info()
            error_msg = "{0}".format({
                'crash_tb': ''.join(traceback.format_tb(exc_tb)),
                'crash_exc': traceback.format_exception_only(
                    exc_type, exc_value
                )[0].strip()
            })

            # ignore copy error if error table does not exist
            s3_log, rs_table = log_tuple
            if rs_table == err_tbl_name and \
               exc_value.args[0].find('The specified S3 prefix') != -1 and \
               exc_value.args[0].find('does not exist') != -1:
                result = None
        finally:
            if result is False:
                _, rs_table = log_tuple
                if error_msg is None:
                    error_msg = "failed copy {0} for date: {1}".format(
                        get_namespaced_tablename(rs_table), ddate
                    )
                status_helper.update_status(
                    db_name, ddate, yaml_versions,
                    "error", start_time_secs=start, error_msg=error_msg
                )
                handle_error(error_msg, logstream)
    status_helper.update_status(
        db_name, ddate, yaml_versions, "complete", start_time_secs=start
    )


def parse_command_line(sys_argv):
    """
    parse_command_line parses the arguments from the command line other than
    the name of the file

    Args:
    sys_argv -- sys.argv

    Returns:
    a namespace of arguments
    """

    parser = get_base_parser()
    parser = add_load_args(parser)
    parser.add_argument(
        "--date",
        help="either 'yesterday' or YYYY-MM-DD \
             if there is no date, s3_to_redshift checks back 5 days"
    )
    # skip the file name, parse everything after
    return parser.parse_args(sys_argv[1:])


def s3_to_redshift_main(args):

    db = read_string('pipeline.redshift_database')
    s3_log_prefix = read_string('pipeline.s3_output_prefix').format(
        logname=os.environ.get('LOGNAME', 'unknown')
    )

    # setup logging
    stream_name = read_string('pipeline.load_step.s3_to_redshift_stream')
    LOG_STREAM = PipelineStreamLogger(
        stream_name,
        args.run_local,
        's3_to_redshift',
        job_name='load'
    )

    # handle to redshift db
    loader_psql = RedshiftPostgres(
        LOG_STREAM, args.private, run_local=args.run_local
    )

    if args.skip_progress_in_redshift:
        status_table = DynamoDbStatusTable(
            LOG_STREAM, run_local=args.run_local
        )
    else:
        status_table = RedshiftStatusTable(loader_psql)

    create_tuples = get_table_creates(args.db_file, LOG_STREAM)

    data_candidates = dates_from_rs_status(
        status_table,
        db,
        LOG_STREAM,
        args.retry_errors,
        args.date,
    )
    if data_candidates:
        try:
            update_database_schema(
                loader_psql,
                db,
                data_candidates[0],
                s3_log_prefix,
                args.db_file,
                LOG_STREAM
            )
        except Exception as e:
            status_table.update_status(
                db,
                data_candidates[0],
                get_yaml_table_versions(pipeline_yaml_schema_file_path()),
                "error",
                start_time_secs=time.time(), error_msg=repr(e)
            )
            raise
    elif args.date is not None:
        raise IOError("{0} data is either already loaded \
or has not yet completed ET step".format(args.date))

    logs_to_copy = []
    for input_date in data_candidates:
        LOG_STREAM = PipelineStreamLogger(
            stream_name,
            args.run_local,
            's3_to_redshift',
            job_name='load',
            input_date=input_date
        )
        logs_to_copy = [
            (join(s3_log_prefix, input_date, table), table)
            for (table, _) in create_tuples
        ]
        copy_tables(loader_psql, status_table, db, input_date, logs_to_copy,
                    args.ttl_days, LOG_STREAM)

if __name__ == '__main__':

    args_namespace = parse_command_line(sys.argv)

    load_package_config(args_namespace.config)
    YamlConfiguration(args_namespace.io_yaml, optional=False)
    if args_namespace.config_override:
        YamlConfiguration(args_namespace.config_override, optional=False)

    s3_to_redshift_main(args_namespace)
