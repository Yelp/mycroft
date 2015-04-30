# -*- coding: utf-8 -*-
"""
util.py holds functions used in more than one module.
NOTE: this code is uploaded to emr with mrjobs
"""

import cPickle
import functools
import boto.s3
from boto.exception import S3ResponseError

from sherlock.common.aws import get_boto_creds


class memoized(object):
    """Decorator that caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned, and
    the function is not re-evaluated.

    Based upon from http://wiki.python.org/moin/PythonDecoratorLibrary#Memoize
    Nota bene: this decorator memoizes /all/ calls to the function.  For a memoization
    decorator with limited cache size, consider:
    http://code.activestate.com/recipes/496879-memoize-decorator-function-with-cache-size-limit/
    """
    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args, **kwargs):
        # If the function args cannot be used as a cache hash key, fail fast
        key = cPickle.dumps((args, kwargs))
        try:
            return self.cache[key]
        except KeyError:
            value = self.func(*args, **kwargs)
            self.cache[key] = value
            return value

    def __repr__(self):
        """Return the function's docstring."""
        return self.func.__doc__

    def __get__(self, obj, objtype):
        """Support instance methods."""
        return functools.partial(self.__call__, obj)


def get_deep(x, path, default=None):
    """ access value of a multi-level dict in one go.
    :param x: a multi-level dict
    :param path: a path to desired key in dict
    :param default: a default value to return if no value at path

    Examples:
    x = {'a': {'b': 5}}
    get_deep(x, 'a.b') returns 5
    get_deep(x, ['a', 'b']) returns 5
    get_deep(x, 'c', 5) returns 5
    """
    if path is None or path == '':
        path_keys = []
    elif type(path) in (list, tuple):
        path_keys = path
    else:
        path_keys = path.split('.')

    v = x or {}
    for k in path_keys:
        try:
            v = v.get(k)
        except TypeError:
            v = None
        finally:
            if v is None:
                return default
    return v


def is_s3_path(file_path):
    """ Return true if file_path is an S3 path, else false.
    """
    schema, _, rest = file_path.partition('://')
    return schema == 's3'


def parse_s3_path(file_path):
    if not is_s3_path(file_path):
        raise ValueError('{0} is not a valid s3 path'.format(file_path))
    parse_array = file_path.split("/", 3)
    bucket = parse_array[2]
    prefix = parse_array[3]
    return bucket, prefix


def _load_from_s3_region(conn, bucket_name, key_name):
    bucket = conn.get_bucket(bucket_name)
    key = bucket.get_key(key_name)
    if key is None:
        raise ValueError('s3://{0}/{1}: no such file'.format(
            bucket_name, key_name
        ))
    return key.get_contents_as_string()


def load_from_s3_file(s3_uri):
    """Load data from S3
    Useful for loading small config or schema files

    :param s3_uri: path to S3 uri
    :returns: file contents
    """
    _, _, path = s3_uri.partition('://')
    bucket_name, _, key_name = path.partition('/')

    # if region is in a bucket name, put that region first
    def preferred_region(item):
        return item.name not in bucket_name

    boto_creds = get_boto_creds()
    for region in sorted(boto.s3.regions(), key=preferred_region):
        try:
            conn = boto.s3.connect_to_region(region.name, **boto_creds)
            return _load_from_s3_region(conn, bucket_name, key_name)
        except S3ResponseError as e:
            # skip to next region if access is not allowed from this one
            if e.status not in [403, 301]:
                raise
    raise ValueError("{0}: No valid region found".format(s3_uri))


def load_from_file(path):
    """Load data from local disk or S3, transparently for caller
    Useful for loading small config or schema files

    :param path: path to file on disk or S3 uri
    :returns: file contents
    """
    if is_s3_path(path):
        return load_from_s3_file(path)
    with open(path, 'r') as f:
        return f.read()
