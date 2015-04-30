# -*- coding: utf-8 -*-
"""
pipeline_common.py holds functions used in more than one other module in the
search data warehouse pipeline.
"""

import argparse
from datetime import date
from datetime import datetime
from datetime import timedelta
import ast
import os
import time

import simplejson
from staticconf import read_string
import yaml
from staticconf import YamlConfiguration, DictConfiguration
from sherlock.common.util import load_from_file
from sherlock.common.util import is_s3_path

MAX_TTL_DAYS = None


class LogMsgFormatter(object):
    """A simple log message formatter that adds
    some useful default fields
    """
    hostname = os.uname()[1]
    pid = os.getpid()

    @staticmethod
    def mk_log_msg(tag, data):
        """A uniform log message in JSON format"""
        return simplejson.dumps({
            # fields starting with _ will appear after @timestamp
            '@timestamp': datetime.isoformat(datetime.utcnow()),
            '_hostname': LogMsgFormatter.hostname,
            '_pid': LogMsgFormatter.pid,
            'msg': data,
            'tag': tag,
        }, sort_keys=True)


class PipelineStreamLoggerBase(object):
    """
    PipelineStreamLoggerBase enables setting up a scribe or file log and writes to
    it, with some parameters commonly used for the sdw pipeline

    Args:
    stream -- the string name of the stream
    run_on_dev -- a boolean for whether this will be run on a dev box or not
    tag -- a string describing the job (generally 'load' or <mr_job_name>)
    kwargs -- any other static arguments to be logged.  Typically it would be:
              input_date='YYYY/MM/DD'
    """

    def __init__(self, stream_name, run_on_dev, tag, **kwargs):
        self.stream = stream_name
        self.tag = tag
        self.kwargs = kwargs
        self.run_on_dev = run_on_dev

    def _pipeline_json(self, status, job_start_secs=None,
                       error_msg=None, extra_msg=None):
        """
        pipeline_json takes some standard arguments and returns a json string
        suitable for logging to scribe or a file

        Args:
        status -- a string like 'complete'
        job_start_secs -- the number of seconds from the epoch the job started
        error_msg -- a string of any error message
        extra_msg -- a string of any extra (non-error) information

        Returns:
        a json string w/ the above info plus what we get from LogMsgFormatter
        """
        data = dict(self.kwargs)
        data['status'] = status
        if job_start_secs:
            data['job_time'] = int(time.time() - job_start_secs)
        if error_msg:
            data['error_msg'] = error_msg
        if extra_msg:
            data['additional_info'] = extra_msg

        return LogMsgFormatter.mk_log_msg(self.tag, data)

    def write_msg(self, status, job_start_secs=None,
                  error_msg=None, extra_msg=None):
        """
        write_msg takes some standard arguments, gets a json string from
        pipeline_json and writes it to a scribe or file log

        Args:
        status -- a string like 'complete'
        job_start_secs -- the number of seconds from the epoch the job started
        error_msg -- a string of any error message
        extra_msg -- a string of any extra (non-error) information

        Returns:
        ---
        """
        raise NotImplementedError


def get_formatted_date(input_date):
    """
    converts dates to the right format.  Note that using Tron we expect an
    input date like 2004-12-05.  We store these dates in form that's nicer to
    use as an S3 path, for example 2004/12/05, so that's the formate we return

    Args:
        input_date -- whatever this gets from the command line

    Returns:
        a date in the right format, or None
    """
    if input_date == "yesterday":
        today = date.today()
        yesterday = today - timedelta(days=1)
        return yesterday.strftime("%Y/%m/%d")
    elif input_date == 'twodaysago':
        today = date.today()
        twodaysago = today - timedelta(days=2)
        return twodaysago.strftime("%Y/%m/%d")
    else:
        try:
            target = datetime.strptime(input_date, "%Y-%m-%d")
            return target.strftime("%Y/%m/%d")
        except ValueError:
            return None


def get_yaml_table_versions(yaml_file):
    """
    get_yaml_table_versions puts together a string of table versions for a
    particular yaml file

    Args:
    yaml_file -- the file with table versions specified

    Returns:
    a string of the form "<tablename>: <versionnumber", ..."
    """
    yaml_data = load_from_file(yaml_file)
    yaml_dict = yaml.load(yaml_data)

    version_list = []
    if 'version' in yaml_dict:
        for table_name in yaml_dict['tables'].keys():
            version_list.append(str(table_name))
        version_list.sort()
        version_list.append(str(yaml_dict['version']))
    else:
        for table_name in yaml_dict.keys():
            version_list.append(
                "{0}: {1}".format(table_name, yaml_dict[table_name]['version'])
            )
        version_list.sort()

    return " ".join(version_list)


def pipeline_yaml_schema_file_path():
    """Return the full path of the yaml schema file for the pipeline. Do
    nothing if the path is already an S3 path
    """
    yaml_schema_file_path = read_string('pipeline.yaml_schema_file')
    if is_s3_path(yaml_schema_file_path):
        return yaml_schema_file_path
    return '{directory}/{filename}'.format(
        directory=os.environ['YELPCODE'],
        filename=yaml_schema_file_path,
    )


def get_base_args_parser():
    """
    get_base_args_parser returns a parser with the following arguments:
        -r, --run-local
        --config
        --config-override
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-r", "--run-local",
        help="-r to run on a dev box",
        default=False,
        action="store_true"
    )
    parser.add_argument(
        "--config",
        help="config.yaml file path",
        required=True
    )
    parser.add_argument(
        "--config-override",
        help="overrides for config file or pipeline file",
        required=False
    )
    return parser


def get_base_parser():
    """
    get_base_parser returns a parser with the following arguments:
       -r, --run-local
       --io_yaml
       --config
       --config-override
       --private
       --skip-progress-in-redshift
    it is meant to be used as a baseline for s3_to_psv, s3_to_redshift and
    ingest_multiple_dates which can then add more specific arguments for
    their implementations
    """
    parser = get_base_args_parser()
    parser.add_argument(
        "--io_yaml",
        help="pipeline specific yaml config - either a path to a local file \
            or a dictionary string",
        required=True
    )
    parser.add_argument(
        "--private",
        help="private yaml file path containing datbase credentials",
    )
    parser.add_argument(
        "--skip-progress-in-redshift",
        help="skip recording progress of ET/L in redshift",
        default=False,
        action="store_true"
    )
    parser.add_argument(
        "--force-et",
        help="force an et even if there's no SUCCESS or COMPLETE file",
        default=False,
        action="store_true"
    )
    return parser


def add_load_args(parser):
    parser.add_argument(
        "db_file",
        help="filename in the schema directory for the create table cmd",
        type=str
    )
    parser.add_argument(
        "--ttl-days", type=int,
        help="how many days to retain loaded data",
        default=MAX_TTL_DAYS,
    )
    parser.add_argument(
        "--retry-errors",
        help="retry load steps that have error'd out.  \
CAUTION -- may duplicate records",
        action="store_true"
    )
    return parser


def load_io_yaml_from_args(io_yaml_arg):
    # Load either a YAML file or a dictionary string for io_yaml
    try:
        arg = ast.literal_eval(io_yaml_arg)
    except ValueError:
        arg = io_yaml_arg

    if type(arg) == str:
        YamlConfiguration(arg, optional=False)
    elif type(arg) == dict:
        DictConfiguration(arg, optional=False)
    else:
        raise ValueError(
            "Arg io_yaml is not a file path or a dictionary, was:"
            + str(type(arg))
        )
