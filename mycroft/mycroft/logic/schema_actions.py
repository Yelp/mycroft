# -*- coding: utf-8 -*-
"""
**logic.schema_actions**
========================

A collection of functions to handle actions relating to schemas
in the mycroft service.  The C part of MVC for mycroft/schemas

"""
from collections import defaultdict
import re
import staticconf

from boto.exception import S3ResponseError

from mycroft.models.aws_connections import get_s3_connection


S3_BUCKET = None
S3_LOG_PREFIX = None
PATH_RE = None
PATH_RE_PREFIX = "{0}/(?P<log_name>[\w]+)/schema/(?P<version>[\w]+)/schema.yaml$"
NAME_VALUE_ERROR = "log_name & log_version must be only [a-zA-Z0-9_]"

# sixty seconds to expiry allows for a reasonable amout of time to download a schema
URL_SECONDS_TO_EXPIRY = 60


def s3_bucket_action(s3_action, *args, **kwargs):
    """
    s3_bucket_action takes a function and the args and kwargs that follow the
    bucket argument for that function.  It gets a connection, and a bucket
    from that connnection, and calls the function with that bucket as an input
    argument.  It then returns the results and closes the connection.

    There are two reasons to do it this way.  First to ease testing.  Each of
    the input functions can be tested separately with a dummy bucket, and this
    function can be tested with a dummy input function.  Second, writing this
    closure allows re-use of the code to get the connection, bucket, and close
    them.

    :param s3_action: a function to be wrapped with bucket as its first arg
    :param \*args: the args following bucket that are inputs to the s3_action
    :type \*args: correspond to the input args of s3_action
    :param \*\*kwargs: the keyword args following bucket and \*args in the
     s3_action call
    :type \*\*kwargs: correspond to the input keyword args of s3_action
    :returns: the output of the function
    """
    try:
        conn = get_s3_connection()
        bucket = conn.get_bucket(S3_BUCKET)
        return s3_action(bucket, *args, **kwargs)
    finally:
        if conn is not None:
            conn.close()


def list_versions_by_log_name(bucket, log_name):
    """
    lists all log versions for a given log name

    :param bucket: S3 bucket object
    :type bucket: boto.s3.bucket.Bucket
    :param log_name: name of the log in s3 (e.g., ad_click)
    :type log_name: string

    :returns: a dict {log_name: 'name', versions: ['x','y']}
    :rtype: dict

    :raises ValueError: if the log_name is invalid
    """
    if re.match("[\w]+", log_name) is None:
        raise ValueError("invalid log_name")
    prefix = "{0}/{1}".format(S3_LOG_PREFIX, log_name)
    log_name_to_versions = _get_log_versions_from_list(bucket, prefix)
    return {'log_name': log_name, 'versions': log_name_to_versions[log_name]}


def list_all_log_versions(bucket):
    """
    lists all log versions

    :param bucket: S3 bucket object
    :type bucket: boto.s3.bucket.Bucket
    :returns: a dict {'schemas': [{log_name: 'name', versions: ['x','y']}]}
    :rtype: dict
    """
    log_name_to_versions = _get_log_versions_from_list(bucket, S3_LOG_PREFIX)
    return {'schemas': [{'log_name': lg_name, 'versions': lg_versions}
            for lg_name, lg_versions
            in log_name_to_versions.iteritems()]}


def _get_log_versions_from_list(bucket, prefix):
    """
    :param bucket: S3 bucket object
    :type bucket: boto.s3.bucket.Bucket
    :param prefix: a prefix of the s3 path to narrow the list
    :type prefix: string

    :returns: a dict {log_name: [versions]}
    :rtype: dict
    """

    s3_list = bucket.list(prefix=prefix)
    log_name_to_versions = defaultdict(list)
    for s3_path in s3_list:
        s3_path_match = PATH_RE.match(s3_path.name)
        if s3_path_match:
            log_name = s3_path_match.group('log_name')
            version = s3_path_match.group('version')
            log_name_to_versions[log_name].append(version)
    return log_name_to_versions


def _get_key_name(log_name, log_version):
    """
    _get_key_name constructs a key_name from the log_name and log_version
    in the arguments and returns it if it's acceptable (has characters in
    the set [a-zA-z0-9_]).  Otherwise raises a ValueError.

    :param log_name: name of the log in s3 (e.g., ad_click)
    :type log_name: string
    :param log_version: version of the log in s3 (e.g,. initial)
    :type log_version: string

    :returns: an S3 Key name
    :rtype: string

    :raises ValueError: if the log_version or log_name is invalid
    """
    key_name = "{0}/{1}/schema/{2}/schema.yaml".format(
        S3_LOG_PREFIX,
        log_name,
        log_version
    )
    if log_name is None or log_version is None or PATH_RE.match(key_name) is None:
        raise ValueError(NAME_VALUE_ERROR)
    return key_name


def get_key_url(bucket, log_name, log_version):
    """
    get_key_url gets a signed url for a key in a given bucket from the
    log_name and log_version arguments

    An example url looks like this:
    https://aws_url/key_path?Signature=xxx%3D&Expires=1407976836\
&AWSAccessKeyId=AKIA&x-amz-meta-s3cmd-attrs=uid%user_id&more=more

    :param bucket: S3 bucket object
    :type bucket: boto.s3.bucket.Bucket
    :param log_name:  name of the log
    :type log_name: string
    :param log_version:  version of the log
    :type log_version: string

    :returns: {'log_name': name, 'log_version': version, 'url', signed_url}
    :rtype: dict

    :raises S3ResponseError: if the bucket/key pair aren't in S3
    """
    key_name = _get_key_name(log_name, log_version)
    s3_key = bucket.get_key(key_name)
    if s3_key is None:
        raise S3ResponseError(
            404,
            "{0} {1} pair is not in s3".format(bucket.name, key_name)
        )
    s3_url = s3_key.generate_url(URL_SECONDS_TO_EXPIRY)
    return {'log_name': log_name, 'log_version': log_version, 'url': s3_url}


def post_key(bucket, log_name, log_version, content):
    """
    post key creates a key from the log_name and log_version input, and
    creates the content for that key from the content in the kwargs.

    :param bucket: S3 bucket object
    :type bucket: boto.s3.bucket.Bucket
    :param log_name:  name of the log
    :type log_name: string
    :param log_version:  version of the log
    :type log_version: string

    :returns: {'log_name': name, 'log_version': version, 'bytes_written', bytes}
    :rtype: dict

    :raises S3ResponseError: if the bytes written don't match the length of
        the content, if the connection failed, or if rejecting an overwrite
    """
    key_name = _get_key_name(log_name, log_version)
    s3_key = bucket.new_key(key_name)
    bytes_written = s3_key.set_contents_from_string(content, replace=False)
    if bytes_written is None:
        raise S3ResponseError(500, "POST failed - overwrite or connection failure")
    elif bytes_written != len(content):
        raise S3ResponseError(502, "POST failed - incomplete")
    return {'log_name': log_name,
            'log_version': log_version,
            'bytes_written': bytes_written}


def init():
        # Generate the configures in advance and cache them to avoid repeated processing
        global S3_BUCKET
        global S3_LOG_PREFIX
        global PATH_RE
        S3_BUCKET = staticconf.read_string('s3_bucket')
        S3_LOG_PREFIX = staticconf.read_string('s3_log_prefix')
        PATH_RE = re.compile(PATH_RE_PREFIX.format(S3_LOG_PREFIX))
