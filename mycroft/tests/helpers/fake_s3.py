# -*- coding: utf-8 -*-
"""Contains a fake S3 implementation.

This is based heavily on boto's mock_storage service, and largely tries to stay
consistent with it:
https://github.com/boto/boto/blob/master/tests/integration/s3/mock_storage_service.py

The main difference is that this fake S3 implementation tries to emulate
eventual consistency of lists.

None of these fakes are complete; instead, they only have enough functionality
implemented to cover what we're using and need to test.
"""

import urllib

import boto.exception
from boto.s3.prefix import Prefix


class FakeConnection(object):

    def __init__(self, aws_access_key_id=None, aws_scret_access_key=None):
        self.buckets = {}

    def get_bucket(self, bucket_name):
        if bucket_name not in self.buckets:
            raise boto.exception.StorageResponseError(404, 'NoSuchBucket')
        return self.buckets[bucket_name]

    def create_bucket(self, bucket_name):
        if bucket_name in self.buckets:
            raise boto.exception.StorageCreateError(
                409,
                'BucketAlreadyOwnedByYou')
        fake_bucket = FakeBucket(name=bucket_name)
        self.buckets[bucket_name] = fake_bucket
        return fake_bucket


class FakeBucket(object):
    """Class to fake a Boto S3 bucket object.

    FakeBuckets will try to simulate S3's eventual consistency for lists.
    On real S3, a `list` operation isn't guaranteed to return a superset
    of earlier `list` results until after X number of hours. We don't want
    to introduce time-based dependencies in our unit-tests, so instead
    we'll simulate eventual consistency by forcing the first X number of
    `lists` to only return partial results.

    Unfortunately, eventual consistency means that there are very few
    guarantees that are assertable in a unit test. In particular, a
    `list` is not guaranteed

    to return any results at all, which makes it very difficult to test.
    To get around that, there is a 'force_consistent_list()' method; after
    it's called on the bucket, all future lists will return 'consistent'
    results.
    """
    # These many `list` operations on the bucket will be 'inconsistent'.
    NUM_INCONSISTENT_LISTS = 4

    def __init__(self, name):
        self.name = name
        self.keys = {}
        # Our eventual consistency is based on number of lists done
        self.list_count = 0
        self.get_key_count = 0
        self._force_consistent_list = False

    def __repr__(self):
        return 'FakeBucket: %s' % self.name

    def force_consistent_list(self):
        """Force all list calls after this to return 'consistent' results.

        You should generally not call this from your application logic;
        instead, your application code should handle inconsistent lists
        gracefully.

        Returns:
            self, for chaining
        """
        self._force_consistent_list = True
        return self

    def new_key(self, key_name=None):
        if key_name in self.keys:
            return self.keys[key_name]
        fake_key = FakeKey(self, key_name)
        self.keys[key_name] = fake_key
        return fake_key

    def _filter_results_for_eventual_consistency(self, results):
        """Simulates S3's eventual consistency by filtering out items from the
        result set.

        Results are filtered out by only returning every n-th result. This has
        the nice property of interleaving returned results, so consumers can't
        make assumptions that treat S3 lists as append only.

        The initial list call also won't return any results if there's only a
        single item in the list, which will make it harder for tests to ignore
        consistency issues.

        Args:
            results - list of results from a list operation

        Returns:
            a list of results made by dropping every n-th result
        """
        if self.list_count > self.NUM_INCONSISTENT_LISTS:
            return results
        else:
            num_remaining_lists = self.NUM_INCONSISTENT_LISTS - self.list_count
            # Don't start from offset of 0, so short lists (which will be more
            # common in tests) won't be able to ignore consistency issues.
            filtered_results = results[num_remaining_lists::self.NUM_INCONSISTENT_LISTS]
            return filtered_results

    def list(self, prefix='', delimiter=''):
        self.list_count += 1
        prefix = prefix or ''  # Turn None into '' for prefix match.

        # Return list instead of using a generator so we can drop certain
        # results when faking eventual consistency.
        results = []
        key_name_set = set()
        for k in sorted(self.keys.itervalues()):
            if k.name.startswith(prefix):
                k_name_past_prefix = k.name[len(prefix):]
                if delimiter:
                    pos = k_name_past_prefix.find(delimiter)
                else:
                    pos = -1
                if (pos != -1):
                    key_or_prefix = FakePrefix(
                        bucket=self, name=k.name[:len(prefix) + pos + 1])
                else:
                    key_or_prefix = k
                if key_or_prefix.name not in key_name_set:
                    # We only want a single Prefix per unique prefix name, even
                    # if there are multiple keys that match the prefix name
                    key_name_set.add(key_or_prefix.name)
                    results.append(key_or_prefix)

        if self._force_consistent_list:
            return results
        else:
            return self._filter_results_for_eventual_consistency(results)

    def get_key(self, key_name, validate=True):
        self.get_key_count += 1
        if key_name in self.keys:
            key = self.keys[key_name]
        elif not validate:
            key = self.new_key(key_name)
        else:
            key = None
        return key


class FakeBucketWithBadConnection(FakeBucket):

    def new_key(self, key_name=None):
        if key_name in self.keys:
            return self.keys[key_name]
        fake_key = FakeKeyWithBadConnection(self, key_name)
        self.keys[key_name] = fake_key
        return fake_key


class KeyOrPrefixEqualityMixin(object):
    """Mixin to provide some equality operations between fake keys/prefixes.

    Having equality operations defined is useful for assertions in unit tests.
    """
    def __repr__(self):
        if self.bucket:
            return '<FakeKeyOrPrefix: %s,%s>' % (self.bucket.name, self.name)
        else:
            return '<FakeKeyOrPrefix: %s>' % self.name

    @property
    def __to_tuple(self):
        """Define a simple way to convert the object to a tuple.

        The tuple will be all of the core attributes that we care about. This
        tuple will then be used for hashing and equality operations.
        """
        return (self.bucket, self.name)

    def __hash__(self):
        return hash(self.__to_tuple)

    def __eq__(self, other):
        if type(self) is type(other):
            return self.__to_tuple == other.__to_tuple
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)


class FakeKey(KeyOrPrefixEqualityMixin):

    def __init__(self, bucket, name,
                 size=206359546, etag=u'"1f7384da352133da523d8eafcf094e92-29"',
                 last_modified=u'2014-02-03T05:36:58.000Z', content=None):

        self.bucket = bucket
        self.name = name
        self.size = size
        self.etag = etag
        self.last_modified = last_modified
        self.data = None

    def get_contents_as_string(self):
        return str(self.data)

    def set_contents_from_string(self, string_data, headers=None,
                                 replace=True, cb=None, num_cb=10,
                                 policy=None, md5=None,
                                 reduced_redundancy=False, encrypt_key=False):
        if self.data is not None:
            return None
        else:
            self.data = string_data
            self.size = len(self.data)
            return self.size

    def generate_url(self, expires_in):
        """
        Args:
        expires_in -- time in seconds the URL is valid

        Returns:
        a string url to download the document

        """
        output_dict = {'name': self.name, 'bucket': self.bucket.name}
        return urllib.urlencode(output_dict)


class FakePrefix(Prefix, KeyOrPrefixEqualityMixin):
    """Sub-class of a real Prefix, with additional equality operations
    implemented.  Real Prefix objects are very light-weight, so there isn't
    any need to fake parts of them out.
    """


class FakeKeyWithBadConnection(FakeKey):

    def set_contents_from_string(self, string_data, headers=None,
                                 replace=True, cb=None, num_cb=10,
                                 policy=None, md5=None,
                                 reduced_redundancy=False, encrypt_key=False):
        if self.data is not None:
            return None
        else:
            self.data = string_data
            self.size = len(self.data)
            return -5
