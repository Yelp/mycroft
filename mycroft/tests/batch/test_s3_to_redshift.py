# -*- coding: utf-8 -*-
from datetime import datetime
import mock
import pytest
import staticconf.testing

from sherlock.batch.s3_to_redshift import get_create_commands
from sherlock.batch.s3_to_redshift import get_column_defaults
from sherlock.batch.s3_to_redshift import handle_error
from sherlock.batch.s3_to_redshift import one_day_greater
from sherlock.batch.s3_to_redshift import dates_from_rs_status
from sherlock.batch.s3_to_redshift import parse_command_line
from sherlock.batch.s3_to_redshift import pipeline_yaml_schema_file_path
from sherlock.batch.s3_to_redshift import LOAD
from sherlock.batch.s3_to_redshift import QUERY_GET_SORT_COLUMN
from sherlock.batch.s3_to_redshift import QUERY_GET_MIN_MAX_DATE
from sherlock.batch.s3_to_redshift import QUERY_DELETE_ROWS_BY_DATE
from sherlock.batch.s3_to_redshift import QUERY_TABLE_DEF
from sherlock.common.redshift_status import RedshiftStatusTable
from sherlock.common.redshift_status import QUERY_COMPLETE_JOB
from sherlock.common.redshift_status import QUERY_COMPLETE_JOBS
from sherlock.common.loggers import PipelineStreamLogger


@pytest.mark.parametrize("input_value", [
    "tests/data/s3_to_r_test_schema.sql",
    "tests/data/s3_to_r_test_schema.yaml",
    "tests/data/nosortkey_test_schema.yaml",
    "tests/data/nosortkey_test_schema.sql"])
def test_get_create_commands(input_value):
    expected_table_to_create = {
        'table_1': 'create table  table_1(\n "id" int8 not null\n) SORTKEY(id);',
        'table_2': 'create table  table_2(\n "id" int8 not null\n) SORTKEY(id);',
        'table_3': 'create table  table_3(\n "id" int8 not null\n);',
    }
    for tbl, actual_create in get_create_commands(input_value, add_error_table=False):
        assert actual_create.lower()[:-1] == expected_table_to_create[tbl].lower()


@pytest.mark.parametrize("input_value", ["error message"])
def test_handle_error(input_value):
    test_logger = PipelineStreamLogger("test_stream", False, "test_tag")
    with pytest.raises(Exception):
        handle_error(input_value, test_logger)


@pytest.mark.parametrize("recent, old, expected_value", [
    ("2014/04/03", "2014/04/02", True),
    ("2014/04/04", "2014/04/02", False),
    ("2014/04/02", "2014/04/03", False),
    ("2014/04/02", "2014/04/02", False),
])
def test_one_day_greater(recent, old, expected_value):
    output_under_test = one_day_greater(recent, old)
    assert output_under_test == expected_value


@pytest.mark.parametrize("table, expected_value", [
    ({'columns': [{'name': 'a', 'sql_attr': 'default 15'}]}, '15'),
    ({'columns': [{'name': 'a', 'sql_attr': 'default (15)'}]}, '(15)'),
    ({'columns': [{'name': 'a', 'sql_attr': 'default (15 12)'}]}, '(15 12)'),
    ({'columns': [{'name': 'a', 'sql_attr': 'default "15)'}]}, '"15)'),
    ({'columns': [{'name': 'a', 'sql_attr': 'default "15"'}]}, '"15"'),
    ({'columns': [{'name': 'a', 'sql_attr': 'default \'15\''}]}, '\'15\''),
])
def test_get_column_defaults(table, expected_value):
    defaults = get_column_defaults(table)
    assert defaults['a'] == expected_value


def test_sql_statements():
    query = LOAD % ('table', 'path', '|')
    assert query == "copy table from 'path/part' CREDENTIALS \
'aws_access_key_id=%s;aws_secret_access_key=%s;token=%s' \
delimiter '|' ESCAPE gzip TRUNCATECOLUMNS TIMEFORMAT as 'epochmillisecs' \
NULL AS '\\0' STATUPDATE ON;"

    query = QUERY_COMPLETE_JOB % \
        {'table_versions': 'table 1', 'data_date': '2014/02/29'}
    assert query.lower() == "select data_date, load_status \
from sdw_pipeline_status where et_status='complete' \
and table_versions=table 1 and data_date=2014/02/29"

    query = QUERY_COMPLETE_JOBS % \
        {'start_ts': '2014-02-29', 'table_versions': 'table 1'}
    assert query.lower() == "select data_date, load_status \
from sdw_pipeline_status where data_date >= 2014-02-29 \
and et_status = 'complete' and table_versions = table 1 \
order by data_date asc"

    query_params = {'tablename': 'tbl', 'schemaname': 'skma'}
    query = QUERY_GET_SORT_COLUMN % query_params
    assert query.lower() == "select \"column\", type from pg_table_def \
where tablename = tbl and sortkey = 1 and schemaname = skma"

    query_params = {'tablename': 'tbl', 'schemaname': 'skma'}
    query = QUERY_TABLE_DEF % query_params
    assert query.lower() == 'select schemaname, tablename, "column", \
type, encoding, distkey, sortkey, "notnull" from pg_table_def where \
tablename=tbl and schemaname=skma'

    query = QUERY_GET_MIN_MAX_DATE.format('testschema.table', 'start')
    assert query.lower() == "select min(start), max(start) from testschema.table"

    query = QUERY_DELETE_ROWS_BY_DATE.format('table', 'time')
    query = query % {'new_min_date': '2014-02-29'}
    assert query.lower() == "delete from table where time < 2014-02-29"


@pytest.mark.parametrize(
    "input_args, expected_value", [
        # run locally (i.e., on a dev box) using --run-local
        (["sherlock/batch/s3_to_redshift.py",
          "--date", "2014-03-20",
          "--private", "private_file.yaml",
          "--config", "config.yaml",
          "--config-override", "override.yaml",
          "--io_yaml", "pipeline_io.yaml",
          "--run-local",
          "db.sql"],
         True),
        # run locally (i.e., on a dev box) using -r
        (["sherlock/batch/s3_to_redshift.py",
          "--date", "2014-03-20",
          "--private", "private_file.yaml",
          "--config", "config.yaml",
          "--config-override", "override.yaml",
          "--io_yaml", "pipeline_io.yaml",
          "-r",
          "db.sql"],
         True),
        # do not run locally (i.e., not on a dev box)
        (["sherlock/batch/s3_to_redshift.py",
          "--date", "2014-03-20",
          "--private", "private_file.yaml",
          "--config", "config.yaml",
          "--config-override", "override.yaml",
          "--io_yaml", "pipeline_io.yaml",
          "db.sql"],
         False)]
    )
def test_parse_command_line_run_local(input_args, expected_value):
    output_under_test = parse_command_line(input_args)
    assert output_under_test.run_local == expected_value


@pytest.mark.parametrize(
    "input_args", [
        # missing requirement db_file
        ["sherlock/batch/s3_to_redshift.py",
         "--date", "2014-03-20",
         "--private", "private_file.yaml",
         "--config", "config.yaml",
         "--config-override", "override.yaml",
         "--io_yaml", "pipeline_io.yaml",
         "-r"],
        # missing requirement config
        ["sherlock/batch/s3_to_redshift.py",
         "--date", "2014-03-20",
         "--private", "private_file.yaml",
         "--config-override", "override.yaml",
         "--io_yaml", "pipeline_io.yaml",
         "-r",
         "db.sql"],
        # missing requirement io_yaml
        ["sherlock/batch/s3_to_redshift.py",
         "--date", "2014-03-20",
         "--private", "private_file.yaml",
         "--config", "config.yaml",
         "--config-override", "override.yaml",
         "-r",
         "db.sql"],
    ])
def test_parse_command_line_missing_args(input_args):
    with pytest.raises(SystemExit):
        parse_command_line(input_args)


def test_pipeline_yaml_schema_file_path():
    directory = 'directory'
    filename = 'filename.yaml'
    config = {
        'pipeline.yaml_schema_file': filename
    }
    with mock.patch.dict('os.environ', {'YELPCODE': directory}):
        with staticconf.testing.MockConfiguration(config):
            assert pipeline_yaml_schema_file_path() == \
                '{directory}/{filename}'.format(
                    directory=directory,
                    filename=filename)


class MockLogger(object):
    # match the input args of PipelineStreamLogger
    def write_msg(
            self,
            status,
            job_start_secs=None,
            error_msg=None,
            extra_msg=None):
        return None


class MockRsPsql(object):
    def __init__(self, run_sql_output):
        self.run_sql_output = run_sql_output

    # match the input args of RedshiftPsql
    def run_sql(self, query, db, message, params=None, output=None):
        return self.run_sql_output


@pytest.mark.parametrize(
    "retry_on_err, single_date, dummy_sql_vals, expected_out",
    [(False,
      None,
      [('2014/07/01', None), ('2014/07/02', 'error'), ('2014/07/03', None)],
      ['2014/07/01']),
     (True,
      None,
      [('2014/07/01', None), ('2014/07/02', 'error'), ('2014/07/03', None)],
      ['2014/07/01', '2014/07/02', '2014/07/03']),
     (False,
      None,
      [('2014/07/03', None)],
      []),
     (False,
      None,
      [('2014/07/01', None), ('2014/07/03', None)],
      ['2014/07/01']),
     (False,
      None,
      [('2014/07/01', None), ('2014/07/02', None)],
      ['2014/07/01', '2014/07/02']),
     (True,
      None,
      [('2014/07/01', None), ('2014/07/02', 'error')],
      ['2014/07/01', '2014/07/02']),
     (False,
      '2014-07-05',
      [('2014/07/05', None)],
      ['2014/07/05'])])
def test_dates_from_rs_status(
        retry_on_err, single_date, dummy_sql_vals, expected_out):
    lggr = MockLogger()
    status_helper = RedshiftStatusTable(MockRsPsql(dummy_sql_vals))
    directory = '.'
    filename = 'schema/db.yaml'

    # since datetime.now is immutable, need to fake out the days to check
    # in order to pretend today is July 6, 2014
    day_delta = (datetime.utcnow() - datetime(year=2014, month=7, day=2)).days
    config = {'pipeline.load_step.days_to_check': day_delta,
              'pipeline.yaml_schema_file': filename}
    with mock.patch.dict('os.environ', {'YELPCODE': directory}):
        with staticconf.testing.MockConfiguration(config):
            result = dates_from_rs_status(
                status_helper,
                'dev',
                lggr,
                retry_on_err,
                single_date)
            assert len(result) == len(expected_out)
            assert set(result) == set(expected_out)
