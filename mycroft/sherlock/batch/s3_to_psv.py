#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
s3_to_psv.py is a collection of functions to transform a directory of s3
files to another file within s3 using emr to put it into a form acceptable to
import into redshift

This must be run from sherlock to work properly

example usage with scribe logs:
sherlock/batch/s3_to_psv.py --date 2003-11-01 --private /path/to/private.yaml \
--io_yaml pipeline_io.yaml --config=config.yaml \
--config-override=config-dev.yaml

example usage to run locally:
sherlock/batch/s3_to_psv.py --date 2003-11-01 --private /path/to/private.yaml \
--io_yaml pipeline_io.yaml --run-local true \
--config=config.yaml --config-override=config-dev.yaml

IMPORTANT:
s3_to_psv.py discovers which mr job code to run by reading config key
'pipeline.et_step.mrjob' from pipeline YAML file (--io_yaml switch) or
override YAML file (--config-override switch)

mr job code must contain 'mrjob_create' function (see below):

def mrjob_create(args=None):
    return YourMRJob(args=args)

"""

import os
import re
import sys
import time
import traceback

from boto.s3.connection import S3Connection
from staticconf import read_int
from staticconf import read_list
from staticconf import read_string
from staticconf import YamlConfiguration

from sherlock.common.redshift_psql import RedshiftPostgres
from sherlock.common.redshift_status import RedshiftStatusTable
from sherlock.common.dynamodb_status import DynamoDbStatusTable
from sherlock.common.aws import get_boto_creds
from sherlock.common.pipeline import get_base_parser
from sherlock.common.pipeline import get_formatted_date
from sherlock.common.pipeline import get_yaml_table_versions
from sherlock.common.pipeline import pipeline_yaml_schema_file_path
from sherlock.common.loggers import PipelineStreamLogger
from sherlock.common.config_util import load_package_config
from sherlock.common.util import parse_s3_path


MAX_CORES = 50


def get_s3_output_user_prefix():
    return read_string('pipeline.s3_output_prefix').format(
        logname=os.environ.get('LOGNAME', 'unknown')
    )


def create_emr_args(date_with_slashes, cores, infile_prefix, local):
    """creates a string containing arguments for mr job

    inputs:
        date_with_slashes -- a date string of the form 'YYYY/MM/DD'
        cores -- the number of cores to use for a conversion
        infile_prefix -- the prefix to the search bucket
        delimiter -- column delimiter for S3 output

    outputs:
        string containing arguments used by ET mr job"""

    input_file = infile_prefix + date_with_slashes +\
        read_string('pipeline.et_step.s3_input_suffix')
    user_prefix = get_s3_output_user_prefix()
    output_file = os.path.join(user_prefix, date_with_slashes)

    if int(cores) > MAX_CORES:
        cores = MAX_CORES

    extractions = pipeline_yaml_schema_file_path()
    delimiter = read_string('redshift_column_delimiter')
    if local:
        template = read_string('run_local.mrjob_arg_template')
    else:
        template = read_string('run_service.mrjob_arg_template')

    return template.format(
        input_file, output_file, cores, extractions, delimiter
    )


def __run_mr_job(mrjob_path, mrjob_arg_str, logstream):
    """
    Extracts mrjob_file (i.e. mr code to run) and
    mrjob constructor arguments from command string

    Loads mrjob_file, constructs mrjob object
    via 'mrjob_create' entry point

    Run mrjob

    Args:

        mrjob_path -- module.entry_point string which is
            used to create and run mrjob object
        mrjob_arg_str -- the command string containing
            mrjob constructor arguments
        logstream -- a PipelineStreamLogger

    Returns:
        True / False -- for success / failure of the execution
        reason -- a reason for failure if any

    outputs:
        output is logged to the logfile for the start and end of the job
    """

    start_epoch = time.time()
    arg_list = mrjob_arg_str.split()
    logstream.write_msg("start")

    module_path_parts = mrjob_path.split('.')
    entry_point = module_path_parts.pop()
    module_name = '.'.join(module_path_parts)
    module = __import__(module_name, globals(), locals(), [entry_point])
    jobid = None

    try:
        logstream.write_msg(
            "running",
            extra_msg="{0} {1}({2})".format(module_name, entry_point, arg_list)
        )
        mr_job = getattr(module, entry_point)(args=arg_list)
        mr_job.set_up_logging(stream=open(os.devnull, 'w'))
        with mr_job.make_runner() as runner:
            try:
                runner.run()
            finally:
                jobid = runner.get_emr_job_flow_id()
        logstream.write_msg(
            "finished",
            job_start_secs=start_epoch,
            extra_msg="job: {0}".format(jobid)
        )
    except KeyboardInterrupt:
        raise
    except:
        exc_type, exc_value, exc_tb = sys.exc_info()
        error_info = {
            'crash_tb': ''.join(traceback.format_tb(exc_tb)),
            'crash_exc': traceback.format_exception_only(
                exc_type, exc_value
            )[0].strip(),
            'job_id': jobid
        }

        logstream.write_msg(
            "error",
            job_start_secs=start_epoch,
            error_msg=error_info
        )
        return (False, "see logs; job: {0}, snippet: {1}".format(
            jobid, error_info['crash_exc']))
    return (True, None)


def data_available(prefix, input_date, local, done_file_name='COMPLETE', force_et=False):
    """
    data_available takes a prefix and input_date and returns a
    True or False depending on whether there is a done_file_name
    in the S3 path

    Args:
    prefix -- the s3 path prefix of the form 's3://bucket/key1/key2/key3/...'
    input_date -- a date string of the form 'YYYY/MM/DD'
    local -- boolean; run on a dev machine or stage
    force_et -- boolean; True to run the mr without the "done_file_name"

    Returns:
    True / False
    """
    if force_et:
        return True

    bucket, prefix_s3 = parse_s3_path(prefix)
    key = prefix_s3 + os.sep + input_date + os.sep + done_file_name
    key = re.sub(os.sep + '+', os.sep, key)  # remove extra slashes if any
    return bucket_key_exists(bucket, key, local)


def bucket_key_exists(bucket_name, key_name, local):
    """
    bucket_key_exists first establishes a connection with S3; from a tron
    server creds are stored in the environment, otherwise, get them from
    the session_file.  Once we have the connection, test if the bucket
    exists, and then if the key exists, and return True if both exist
    otherwise return False.

    Args:
    bucket_name -- the name of the bucket
    key_name -- the name of a key
    run_local -- whether this is run on a dev box (run_local=True) or instance

    Returns:
    True if the bucket key pair exists, otherwise False
    """
    if local:
        conn = S3Connection(**get_boto_creds())
    else:
        conn = S3Connection()
    if conn.lookup(bucket_name) is None:
        return False
    s3_bucket = conn.get_bucket(bucket_name, validate=False)
    exists = (s3_bucket.get_key(key_name) is not None)
    conn.close()
    return exists


def get_next_dir_to_load(prefixes, date_with_slashes, local, logstream,
                         force_et=False):
    """
    get_next_dir_to_load gets the prefix from S3 where the data is.

    Args:
    prefixes -- a list of S3 prefixes where the data may be
    date_with_slashes -- a date string of the form 'YYYY/MM/DD'
    local -- boolean whether running locally (dev machine) or not
    logtream -- a PipelineStreamLogger
    force_et -- boolean; True to run the mr without the "done_file_name"

    Returns:
    the prefix of the data, or None if there's none
    """

    for prefix in prefixes:
        logstream.write_msg(
            "running",
            extra_msg="trying this input {0}".format(prefix)
        )
        if data_available(prefix, date_with_slashes, local, force_et=force_et):
            return prefix
        logstream.write_msg(
            "running",
            extra_msg="this input failed: {0}".format(prefix)
        )
    return None


def __load_data_from_s3(
        status_helper, prefixes, date_with_slashes,
        mrjob_path, local, db_name, logstream, force_et=False
        ):
    """
    load_data_from_s3 iterates over prefixes and loads data for a
    particular date for the first prefix where the data exists.  It also
    checks whether data has already been loaded for a date and if so, skips
    the load

    Args:
    status_helper -- An object handle to interact with status table
    prefixes -- a list of s3 prefixes for input data
    date_with_slashes -- a date string of the form 'YYYY/MM/DD'
    mrjob_path -- module.entry_point of the job to extract and \
        transform the data
    local -- True if we're running locally (i.e., devc) False for aws instance
    logstream -- a PipelineStreamLogger

    Returns:
    ---
    """
    start_time = time.time()

    table_versions = get_yaml_table_versions(pipeline_yaml_schema_file_path())
    conditions = {
        'table_versions': table_versions,
        'data_date': date_with_slashes
    }
    if status_helper.et_started(conditions, db_name):
        logstream.write_msg(
            "complete",
            extra_msg="skipping: et_step already started"
        )
        return

    prefix_for_this_data = get_next_dir_to_load(
        prefixes, date_with_slashes, local, logstream, force_et
    )
    if not prefix_for_this_data:
        jobtime = 0
        err_msg = "no prefix available date={0}, prefixes={1}".format(
            date_with_slashes, prefixes
        )
        logstream.write_msg("error", error_msg=err_msg)
        status_helper.log_status_result(
            conditions,
            jobtime,
            db_name,
            failed=True, err_msg=err_msg
        )
        raise Exception(err_msg)

    # check if mrjob is already done
    data_we_check = "{0} {1} {2}".format(
        get_s3_output_user_prefix(),
        date_with_slashes,
        local
    )
    logstream.write_msg("running", extra_msg=data_we_check)
    if data_available(
        get_s3_output_user_prefix(),
        date_with_slashes,
        local,
        done_file_name='_SUCCESS'
    ):
        logstream.write_msg(
            "complete",
            extra_msg="skipping: et_step already done"
        )
        return

    jobtime = time.time()
    mrjob_args = create_emr_args(
        date_with_slashes,
        read_int('pipeline.et_step.cores'),
        prefix_for_this_data, local
    )
    status_helper.insert_et(conditions, db_name)
    logstream.write_msg("running", extra_msg=mrjob_args)

    result, err_reason = __run_mr_job(mrjob_path, mrjob_args, logstream)
    failed = not result

    jobtime = time.time() - start_time
    status_helper.log_status_result(
        conditions, jobtime, db_name,
        failed=failed, err_msg=err_reason
    )
    if failed:
        raise Exception(err_reason)
    return


def clear_env(local):
    """
    clears the environment variables for aws credentials needed to complete the
    run.  Use this on an error or successful completion

    Args:
    local -- True if running on dev, False if running on stageb

    Returns
    ---
    """
    if not local:
        if os.environ.get('AWS_ACCESS_KEY_ID') is not None:
            del os.environ['AWS_ACCESS_KEY_ID']
        if os.environ.get('AWS_SECRET_ACCESS_KEY') is not None:
            del os.environ['AWS_SECRET_ACCESS_KEY']


def setup_private(input_args):
    """
    setup_private sets up the aws credentials required to run on the server
    in the appropriate environment variables

    Args:
    local -- True if we're on dev, False if on stageb
    input_args -- input yaml file with aws access_key_id and secret_access_key

    Returns
    a yaml file with the private information in it
    ---
    """

    YamlConfiguration(input_args, optional=True)
    os.environ['AWS_ACCESS_KEY_ID'] = read_string('emr_aws_access_key_id')
    os.environ['AWS_SECRET_ACCESS_KEY'] = \
        read_string('emr_aws_secret_access_key')


def setup_dates_to_check(date_with_dashes, local, logstream):
    """
    setup_dates_to_check follows this logic:

    if there's a data_date check it and if it's ok return that

    if not then log an error for a bad date raise exception

    Args:
    date_with_dashes -- a date string of the form 'YYYY-MM-DD'
    local -- whether we're running on locally (dev box) or not
    logstream -- a PipelineStreamLogger

    Returns:
    a string of the form YYYY/MM/DD
    """
    input_date = get_formatted_date(date_with_dashes)
    if input_date:
        return input_date

    error_msg = "input date {0} is invalid".format(date_with_dashes)
    logstream.write_msg("error", error_msg=error_msg)
    clear_env(local)
    raise Exception(error_msg)


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
    parser.add_argument(
        "--date",
        help="'yesterday' or 'twodaysago' or YYYY-MM-DD",
        required=True
    )

    # skip the file name, parse everything after
    return parser.parse_args(sys_argv[1:])


def s3_to_psv_main(args):

    mrjob = read_string('pipeline.et_step.mrjob')
    stream_name = read_string('pipeline.et_step.s3_to_s3_stream')
    DATABASE = read_string('pipeline.redshift_database')

    LOG_STREAM = PipelineStreamLogger(
        stream_name,
        args.run_local,
        mrjob,
        input_date=args.date
    )

    day_to_run = setup_dates_to_check(args.date, args.run_local, LOG_STREAM)

    try:
        if not args.run_local:
            setup_private(args.private)
        # Create a psql instance based on args
        if args.skip_progress_in_redshift:
            status_table = DynamoDbStatusTable(
                LOG_STREAM, run_local=args.run_local
            )
        else:
            status_table = RedshiftStatusTable(
                RedshiftPostgres(
                    LOG_STREAM, args.private, run_local=args.run_local
                )
            )
        load_msg = __load_data_from_s3(
            status_table,
            read_list('pipeline.et_step.s3_prefixes'),
            day_to_run,
            mrjob,
            args.run_local,
            DATABASE,
            LOG_STREAM,
            force_et=args.force_et
        )
        LOG_STREAM.write_msg("complete", extra_msg=load_msg)

    finally:
        clear_env(args.run_local)


if __name__ == "__main__":
    args_namespace = parse_command_line(sys.argv)

    load_package_config(args_namespace.config)
    YamlConfiguration(args_namespace.io_yaml, optional=False)
    if args_namespace.config_override:
        YamlConfiguration(args_namespace.config_override, optional=False)

    s3_to_psv_main(args_namespace)
