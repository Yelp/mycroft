# -*- coding: utf-8 -*-
'''
Unit test mycroft.models.et_records public API
'''
from collections import namedtuple
import contextlib
import shlex
import subprocess
import socket

import avro.schema
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import STRING
import pytest

from mycroft.models.abstract_records import PrimaryKeyError
from mycroft.models.etl_records import ETLRecords
from mycroft.models.scheduled_jobs import ScheduledJobs

from tests.data import etl_record
from tests.data import scheduled_job


_BASE_PORT = 15000


def next_open_port():
    '''Find an open port between range 15000 to 16000 on localhost

    :returns: an available port to bind to.
    :rtype: int
    '''
    host = 'localhost'
    for port in xrange(_BASE_PORT, _BASE_PORT + 1000):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind((host, port))
            s.close()
            return port
        except socket.error:
            continue
    raise IOError("Cannot find an available port on localhost; unable to start"
                  "local DynamoDB")


@pytest.yield_fixture(scope='session')
def dynamodb_connection():
    '''
    Starts a local DynamoDB

    :returns: a dictionary with the following keys:
        'process': subprocess object
        'port': port number the local DyanmoDB is listening to
    :rtype: dictionary
    '''
    port = next_open_port()
    command_line = 'java ' + \
        '-Djava.library.path=tests/dynamodb_local/DynamoDBLocal_lib ' + \
        '-jar tests/dynamodb_local/DynamoDBLocal.jar ' + \
        '-port {0} -inMemory'.format(port)
    args = shlex.split(command_line)
    p = subprocess.Popen(args)
    yield DynamoDBConnection(
        host='localhost',
        port=port,
        aws_access_key_id='',
        aws_secret_access_key='',
        is_secure=False)
    print "tear down in-memory dynamodb"
    p.kill()


def get_avro_schema(path):
    with open(path, 'r') as f:
        json_text = f.read()
    return avro.schema.parse(json_text)


klass_map = {
    'etl_records': ETLRecords,
    'scheduled_jobs': ScheduledJobs,
}


avro_map = {
    'etl_records': 'mycroft/avro/etl_record.json',
    'scheduled_jobs': 'mycroft/avro/scheduled_jobs.json'
}


CONCRETE_RECORDS_UNDER_TEST = [
    'etl_records',
    'scheduled_jobs',
]


NAME_TO_SCHEMA = {
    'etl_records': [HashKey('hash_key', data_type=STRING),
                    RangeKey('data_date', data_type=STRING)],
    'scheduled_jobs': [HashKey('hash_key', data_type=STRING)],
    'redshift_clusters': [HashKey('redshift_id', data_type=STRING)]
}


NAME_TO_HAS_RANGE_KEY = {
    'etl_records': True,
    'scheduled_jobs': False,
}

NAME_TO_SAMPLE_RECORD_IN_DB = {
    'etl_records': etl_record.SAMPLE_RECORD_IN_DB,
    'scheduled_jobs': scheduled_job.SAMPLE_RECORD_IN_DB,
}

NAME_TO_SAMPLE_RECORD_NOT_IN_DB = {
    'etl_records': etl_record.SAMPLE_RECORD_NOT_IN_DB,
    'scheduled_jobs': scheduled_job.SAMPLE_RECORD_NOT_IN_DB,
}

NAME_TO_SAMPLE_RECORD_MISSING_HASH_KEY = {
    'etl_records': etl_record.SAMPLE_RECORD_MISSING_HASH_KEY,
    'scheduled_jobs': scheduled_job.SAMPLE_RECORD_MISSING_HASH_KEY,
}

NAME_TO_SAMPLE_RECORD_MISSING_RANGE_KEY = {
    'etl_records': etl_record.SAMPLE_RECORD_MISSING_RANGE_KEY,
    'scheduled_jobs': None,
}


@pytest.yield_fixture
def records_bundle(request, dynamodb_connection):
    avro_schema = get_avro_schema(avro_map[request.param])
    assert_test_unknown_kwarg_not_in_schema(avro_schema)
    klass = klass_map[request.param]
    table = Table.create(
        klass.__name__,
        schema=NAME_TO_SCHEMA[request.param],
        connection=dynamodb_connection)
    assert table.put_item(NAME_TO_SAMPLE_RECORD_IN_DB[request.param])
    records_object = klass(
        persistence_object=table,
        avro_schema_object=avro_schema)
    yield create_records_bundle(request.param, records_object)
    assert table.delete()


RecordsBundle = namedtuple('RecordsBundle',
                           ['records',
                            'sample_record_in_db',
                            'sample_record_not_in_db',
                            'has_range_key',
                            'sample_record_missing_hash_key',
                            'sample_record_missing_range_key',
                            ])


def create_records_bundle(name, records_object):
    has_range_key = NAME_TO_HAS_RANGE_KEY[name]
    sample_record_in_db = NAME_TO_SAMPLE_RECORD_IN_DB[name]
    sample_record_not_in_db = NAME_TO_SAMPLE_RECORD_NOT_IN_DB[name]
    sample_record_missing_hash_key = NAME_TO_SAMPLE_RECORD_MISSING_HASH_KEY[name]
    sample_record_missing_range_key = NAME_TO_SAMPLE_RECORD_MISSING_RANGE_KEY[name]

    return RecordsBundle(
        records=records_object,
        sample_record_in_db=sample_record_in_db,
        sample_record_not_in_db=sample_record_not_in_db,
        has_range_key=has_range_key,
        sample_record_missing_hash_key=sample_record_missing_hash_key,
        sample_record_missing_range_key=sample_record_missing_range_key)


def pytest_generate_tests(metafunc):
    if 'records_bundle' in metafunc.fixturenames:
        metafunc.parametrize("records_bundle",
                             CONCRETE_RECORDS_UNDER_TEST,
                             indirect=True)


UNKNOWN_KWARG = 'color'


def assert_test_unknown_kwarg_not_in_schema(avro_schema):
    names = [field.name for field in avro_schema.fields]
    assert UNKNOWN_KWARG not in names


class TestRecords(object):

    def test_get_with_no_kwargs(self, records_bundle):
        with pytest.raises(ValueError):
            records_bundle.records.get()

    def test_get_with_unknown_kwarg(self, records_bundle):
        kwargs = {UNKNOWN_KWARG: None}
        with pytest.raises(ValueError):
            records_bundle.records.get(**kwargs)

    def test_get_with_single_kwarg(self):
        pass

    def test_get_with_multiple_kwargs(self, records_bundle):
        if not records_bundle.has_range_key:
            return
        kwargs = dict(records_bundle.sample_record_in_db)
        assert len(kwargs) > 1
        record = records_bundle.records.get(**kwargs)
        assert record
        record_dictionary = record.get(**kwargs)
        assert all([record_dictionary[key] == kwargs[key]
                    for key in kwargs.iterkeys()])

    def test_get_with_no_matching_record(self, records_bundle):
        with pytest.raises(KeyError):
            records_bundle.records.get(**records_bundle.sample_record_not_in_db)

    def test_get_with_missing_hash_key(self, records_bundle):
        with pytest.raises(PrimaryKeyError):
            records_bundle.records.get(**records_bundle.sample_record_missing_hash_key)

    def test_get_with_missing_range_key(self, records_bundle):
        if not records_bundle.has_range_key:
            return
        with pytest.raises(PrimaryKeyError):
            records_bundle.records.get(**records_bundle.sample_record_missing_range_key)

    def test_put_with_no_kwargs(self, records_bundle):
        with pytest.raises(ValueError):
            records_bundle.records.put()

    def test_put_with_unknown_kwargs(self, records_bundle):
        kwargs = {UNKNOWN_KWARG: None}
        with pytest.raises(ValueError):
            records_bundle.records.put(**kwargs)

    def test_put_with_missing_hash_key(self, records_bundle):
        with pytest.raises(PrimaryKeyError):
            records_bundle.records.put(**records_bundle.sample_record_missing_hash_key)

    def test_put_with_missing_range_key(self, records_bundle):
        if not records_bundle.has_range_key:
            return
        with pytest.raises(PrimaryKeyError):
            records_bundle.records.put(**records_bundle.sample_record_missing_range_key)

    def test_put_with_existing_record(self, records_bundle):
        with pytest.raises(ValueError):
            records_bundle.records.put(**records_bundle.sample_record_in_db)

    def test_put_succeed(self, records_bundle):
        success = records_bundle.records.put(**records_bundle.sample_record_not_in_db)
        assert success

    def test_put_fail(self, records_bundle):
        pass

    def test_query_by_index_with_too_many_components(self, records_bundle):
        with pytest.raises(ValueError):
            records_bundle.records._records.query_by_index(index=None, a='1', b='2', c='3')

    def test_iterator_with_one_element(self, records_bundle):
        all_records = [record for record in records_bundle.records]
        simple_dicts = [record.get(**records_bundle.sample_record_in_db) for record in all_records]
        assert simple_dicts == [records_bundle.sample_record_in_db]

    def test_iterator_with_two_elements(self, records_bundle):
        success = records_bundle.records.put(**records_bundle.sample_record_not_in_db)
        assert success
        all_records = [record for record in records_bundle.records]
        simple_dicts = [record.get(**records_bundle.sample_record_in_db) for record in all_records]
        expected = [records_bundle.sample_record_in_db, records_bundle.sample_record_not_in_db]
        assert len(simple_dicts) == len(expected)
        assert [elm for elm in simple_dicts if elm not in expected] == []

    def test_iterator_with_no_elements(self, records_bundle):
        assert records_bundle.records._records._persistence_object.delete_item(
            **records_bundle.sample_record_in_db)
        all_records = [record for record in records_bundle.records]
        simple_dicts = [record.get(**records_bundle.sample_record_in_db) for record in all_records]
        assert simple_dicts == []


@contextlib.contextmanager
def ensure_original_record(records, **kwargs):
    '''
    ensure_original_record is a context manager that yields the record being
    rqeuested. Upon exit, the original values will be restored
    to the original value upon enter.

    Example::
        >>> kwargs = {
                hash_key='1:public:search',
                data_date='2014-07-28'
            }
        >>> with ensure_original_record(records, **kwargs) as record:
                print record.get(etl_status=None)['etl_status']
                assert record.update(etl_status='et_completed')
                print record.get(etl_status=None)['etl_status']
        'et_started'
        'et_completed'
        >>> records.get(**kwargs).get(etl_status=None)['etl_status']
        'et_started' # notice the original value is still in the record
    '''
    record = records.get(**kwargs)
    old_copy = dict(record._record._item.items())
    try:
        yield record
    finally:
        assert record.update(**old_copy)


@pytest.fixture
def old_hash_key(records_bundle):
    record = records_bundle.records.get(**records_bundle.sample_record_in_db)
    return record.get(hash_key=None)['hash_key']


def test_ensure_original_record(records_bundle, old_hash_key):
    with ensure_original_record(records_bundle.records, **records_bundle.sample_record_in_db) \
            as record:
        new_hash_key = '{0} {0}'.format(old_hash_key)
        assert record.update(hash_key=new_hash_key)
        assert new_hash_key == record.get(hash_key=None)['hash_key']
    assert old_hash_key == record.get(hash_key=None)['hash_key']


def test_ensure_original_record_survives_crashes(records_bundle, old_hash_key):
    my_exception = Exception()
    try:
        with ensure_original_record(records_bundle.records, **records_bundle.sample_record_in_db) \
                as record:
            new_hash_key = '{0} {0}'.format(old_hash_key)
            assert record.update(hash_key=new_hash_key)
            assert new_hash_key == record.get(hash_key=None)['hash_key']
            raise my_exception
    except Exception as e:
        # Needs to make sure its the exception we expect
        assert my_exception == e
        assert old_hash_key == record.get(hash_key=None)['hash_key']


class TestETLRecord(object):

    def test_update_succeed(self):
        pass

    def test_update_fail(self):
        pass

    def test_update_with_no_kwargs(self, records_bundle):
        record = records_bundle.records.get(**records_bundle.sample_record_in_db)
        with pytest.raises(ValueError):
            record.update()

    def test_update_with_single_kwarg(self, records_bundle, old_hash_key):
        with ensure_original_record(records_bundle.records, **records_bundle.sample_record_in_db) \
                as record:
            sample_record_copy = dict(**records_bundle.sample_record_in_db)
            new_value = '{0} {0}'.format(old_hash_key)
            assert record.update(hash_key=new_value)
            sample_record_copy.update(hash_key=new_value)
            current_value = records_bundle.records.get(**sample_record_copy)\
                .get(hash_key=None)['hash_key']
            assert current_value == new_value

    def test_update_with_multiple_kwargs(self, records_bundle):
        with ensure_original_record(records_bundle.records, **records_bundle.sample_record_in_db) \
                as record:
            new_dict = dict(record.get(**records_bundle.sample_record_in_db))
            new_dict = dict((key, '{0} {0}'.format(value))
                            for key, value in new_dict.iteritems())
            assert record.update(**new_dict)
            current_dict = records_bundle.records.get(**new_dict)\
                .get(**new_dict)
            assert current_dict == new_dict

    def test_update_with_unknown_kwarg(self, records_bundle):
        record = records_bundle.records.get(**records_bundle.sample_record_in_db)
        with pytest.raises(ValueError):
            record.update(UNKNOWN_KWARG='black')

    def test_delete_suceed(self):
        pass

    def test_delete_fail(self):
        pass

    @pytest.mark.parametrize('user', [
        None,
        '',
        '\t \n',
    ])
    def test_delete_without_user(self, records_bundle, user):
        record = records_bundle.records.get(**records_bundle.sample_record_in_db)
        with pytest.raises(ValueError):
            record.delete(user, 'I just want to delete this')

    @pytest.mark.parametrize('reason', [
        None,
        '',
        '\t \n',
    ])
    def test_delete_without_reason(self, records_bundle, reason):
        record = records_bundle.records.get(**records_bundle.sample_record_in_db)
        with pytest.raises(ValueError):
            record.delete('make test', reason)

    def test_get_with_no_kwargs(self, records_bundle):
        record = records_bundle.records.get(**records_bundle.sample_record_in_db)
        with pytest.raises(ValueError):
            record.get()

    def test_get_with_single_kwarg(self):
        pass

    def test_get_with_multiple_kwargs(self, records_bundle):
        record = records_bundle.records.get(**records_bundle.sample_record_in_db)
        key_to_value = record.get(**records_bundle.sample_record_in_db)
        assert key_to_value == records_bundle.sample_record_in_db

    def test_get_with_unknown_kwarg(self, records_bundle):
        record = records_bundle.records.get(**records_bundle.sample_record_in_db)
        # making a copy of SAMPLE_RECORD_IN_DB because we
        # need to add an unknown key without mutating the original
        # dictionary
        kwargs = dict(records_bundle.sample_record_in_db)
        kwargs[UNKNOWN_KWARG] = None
        with pytest.raises(ValueError):
            record.get(**kwargs)
