# -*- coding: utf-8 -*-
import pytest

from sherlock.batch.s3_to_psv import create_emr_args
from sherlock.batch.s3_to_psv import data_available
from sherlock.batch.s3_to_psv import parse_command_line
from sherlock.common.pipeline import pipeline_yaml_schema_file_path
from sherlock.common.config_util import load_package_config

import mock
import os

from staticconf import read_list
from staticconf import read_string
from staticconf import YamlConfiguration


@pytest.mark.parametrize(
    "input_args, expected_value", [
        # run local (using a dev box) using --run-local
        (["sherlock/batch/s3_to_psv.py",
          "--date", "2014-03-20",
          "--private", "private_file.yaml",
          "--run-local",
          "--force-et",
          "--config", "config.yaml",
          "--io_yaml", "pipeline_io.yaml"],
         True),
        # run local (using a dev box) using -r
        (["sherlock/batch/s3_to_psv.py",
          "--date", "2014-03-20",
          "--private", "private_file.yaml",
          "-r",
          "--force-et",
          "--config", "config.yaml",
          "--io_yaml", "pipeline_io.yaml"],
         True),
        #  do not run on dev box
        (["sherlock/batch/s3_to_psv.py",
          "--date", "2014-03-20",
          "--private", "private_file.yaml",
          "--config", "config.yaml",
          "--io_yaml", "pipeline_io.yaml"],
         False)]
    )
def test_parse_command_line_run_local(input_args, expected_value):
    output_under_test = parse_command_line(input_args)
    assert output_under_test.run_local == expected_value
    assert output_under_test.force_et == expected_value


@pytest.mark.parametrize(
    "input_args", [
        # missing io_yaml
        ["sherlock/batch/s3_to_psv.py",
         "--date", "2014-03-20",
         "--private", "private_file.yaml",
         "--config", "config.yaml",
         "-r"],
        # missing date
        ["sherlock/batch/s3_to_psv.py",
         "--private", "private_file.yaml",
         "--config", "config.yaml",
         "--io_yaml", "pipeline_io.yaml"],
        # missing config
        ["sherlock/batch/s3_to_psv.py",
         "--date", "2014-03-20",
         "--private", "private_file.yaml",
         "--io_yaml", "pipeline_io.yaml",
         "-r"],
    ])
def test_parse_command_line_missing_args(input_args):
    with pytest.raises(SystemExit):
        parse_command_line(input_args)


EXPECTED_DEV_ARGS = """\
--runner=emr {0} \
--output-dir={1} \
--no-output \
--num-ec2-core-instances={2} \
--extractions {3} \
--column-delimiter={4} \
-c /etc/mrjob.minimal.conf \
-c sherlock/config/mrjob.conf"""
EXPECTED_AWS_ARGS = """\
--runner=emr {0} \
--output-dir={1} \
--no-output \
--num-ec2-core-instances={2} \
--extractions {3} \
--column-delimiter={4} \
-c /etc/mrjob.minimal.conf -c sherlock/config/mrjob.conf"""


@pytest.mark.parametrize("input_date, dev, cores, pipeline_yaml",
                         [("2014/04/14",
                           True,
                           10,
                           "pipeline_io.yaml"),
                          ("2014/04/14",
                           False,
                           10,
                           "pipeline_io.yaml"),
                          ("2014/04/14",
                           True,
                           10,
                           "pipeline_io_v4.yaml"),
                          ("2014/04/14",
                           False,
                           10,
                           "pipeline_io_v4.yaml")])
def test_create_emr_args(input_date, dev, cores, pipeline_yaml):
    print "just starting"
    load_package_config('config.yaml')
    YamlConfiguration(pipeline_yaml)

    input_prefix = read_list('pipeline.et_step.s3_prefixes')[0]
    input_file = input_prefix + input_date + '/part-*.gz'

    expected_args = EXPECTED_DEV_ARGS if dev else EXPECTED_AWS_ARGS
    expected_out_file = read_string('pipeline.s3_output_prefix')
    delimiter = read_string('redshift_column_delimiter')
    with mock.patch.dict(os.environ, {'LOGNAME': 'testuser', 'YELPCODE': '.'}):
        logname = os.environ['LOGNAME']
        expected_out_file = os.path.join(
            expected_out_file.format(logname=logname),
            input_date
        )
        extractions = pipeline_yaml_schema_file_path()
        formatted_args = expected_args.format(input_file,
                                              expected_out_file,
                                              cores,
                                              extractions,
                                              delimiter)
        output_under_test = create_emr_args(input_date, 10,
                                            input_prefix, dev)
        assert output_under_test == formatted_args


@pytest.mark.parametrize(
    "bucket_key_exists_rv, force_et, expected_result",
    [(True, True, True),
     (True, False, True),
     (False, True, True),
     (False, False, False)])
def test_data_available_force_et(bucket_key_exists_rv, force_et, expected_result):
    with mock.patch('sherlock.batch.s3_to_psv.bucket_key_exists') as bucket_key_exists:
        bucket_key_exists.return_value = bucket_key_exists_rv
        local_dummy = True
        actual_result = data_available(
            "s3://bucket/key/",  # dummy key
            "1999/12/31",  # dummy date
            local_dummy,
            done_file_name="dummy_done_file_name",
            force_et=force_et
        )
        assert actual_result == expected_result
