# -*- coding: utf-8 -*-
from datetime import datetime
from datetime import timedelta

import mock
import pytest
import simplejson
import staticconf
import staticconf.testing
import time

from sherlock.common.pipeline import get_formatted_date
from sherlock.common.pipeline import get_yaml_table_versions
from sherlock.common.loggers import PipelineStreamLogger
from sherlock.common.pipeline import pipeline_yaml_schema_file_path
from sherlock.common.status import StatusRecord
from sherlock.common.redshift_status import get_update_string
from sherlock.common.redshift_status import get_insert_string
from sherlock.common.pipeline import load_io_yaml_from_args


@pytest.mark.parametrize("input_value, expected_value", [
    ('2014-03-35', None),
    ('2014-13-07', None),
    ('2014-12-15', '2014/12/15')
])
def test_get_formatted_date(input_value, expected_value):
    output_under_test = get_formatted_date(input_value)
    assert output_under_test == expected_value


def test_get_yesterday():
    expected_value = (datetime.now() - timedelta(days=1)).strftime("%Y/%m/%d")
    output_under_test = get_formatted_date('yesterday')
    assert output_under_test == expected_value


def test_status_record_minimized_record():
    expected_value = {'data_date': "2014/12/01"}
    sr = StatusRecord(data_date="2014/12/01")
    assert expected_value == sr.minimized_record()


def test_status_record_get_update_string():
    staticconf.YamlConfiguration("config.yaml")
    sr = StatusRecord(et_status="complete", et_runtime=1000)
    expected_value = "UPDATE sdw_pipeline_status SET et_runtime = "\
                     "%(et_runtime)s, et_status = %(et_status)s WHERE "\
                     "data_date = %(data_date)s AND "\
                     "table_versions = %(table_versions)s"

    output_under_test = get_update_string(sr.get_set_clause())
    assert expected_value == output_under_test


def test_status_record_get_insert_string():
    staticconf.YamlConfiguration("config.yaml")
    sr = StatusRecord(data_date="2014/10/13", et_status="wip",
                      table_versions="search: 1")
    output_under_test = get_insert_string(sr.minimized_record())
    expected_value = "INSERT INTO sdw_pipeline_status (data_date, et_status, "\
        "table_versions) VALUES (%(data_date)s, %(et_status)s, "\
        "%(table_versions)s)"
    assert expected_value == output_under_test


def test_get_twodaysago():
    expected_value = (datetime.now() - timedelta(days=2)).strftime("%Y/%m/%d")
    output_under_test = get_formatted_date('twodaysago')
    assert output_under_test == expected_value


def test_old_get_yaml_table_version():
    expected_value = "aaaa: 1 bbbb: 2 cccc: 3"
    output_under_test = get_yaml_table_versions(
        'tests/common/test_db_old.yaml')
    assert output_under_test == expected_value


def test_new_get_yaml_table_version():
    expected_value = "aaaa bbbb cccc 2"
    output_under_test = get_yaml_table_versions(
        'tests/common/test_db_new.yaml')
    assert output_under_test == expected_value


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


def test_pipeline_yaml_schema_file_path_with_s3():
    filename = 's3://bucket/key/filename.yaml'
    config = {
        'pipeline.yaml_schema_file': filename
    }
    with staticconf.testing.MockConfiguration(config):
        assert pipeline_yaml_schema_file_path() == filename


def test_pipeline_json():
    logger = PipelineStreamLogger(None, False, 'test')
    now = time.time()
    js = logger._pipeline_json('complete',
                               error_msg='error!',
                               extra_msg='extra',
                               job_start_secs=now)
    json_dict = simplejson.loads(js)
    assert json_dict['msg']['status'] == 'complete'
    assert json_dict['msg']['additional_info'] == 'extra'
    # if the above lines of code take longer than 10 seconds something's wrong
    assert json_dict['msg']['job_time'] < 10
    assert json_dict['tag'] == 'test'


def test_load_io_yaml_from_arg_with_string_file_not_found():
    with pytest.raises(IOError):
        load_io_yaml_from_args("string_val")


def test_load_io_yaml_from_int_throws_error():
    with pytest.raises(ValueError):
        load_io_yaml_from_args("4")


def test_load_io_yaml_from_dict():
    dict_str = "{'a': 'value', 'b': 'value'}"
    load_io_yaml_from_args(dict_str)
