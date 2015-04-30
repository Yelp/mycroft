# -*- coding: utf-8 -*-
import pytest

from sherlock.common import schema


def test_constructor():
    log_key = 'actions'
    is_json = True
    sql_attr = 'varchar(65535)'
    is_mandatory = False
    extraction_type = schema.ColumnType.EXTRACTION
    source = 'search'
    is_foreign = True
    is_noop = True
    test_column = schema.Column(log_key=log_key, is_json=is_json,
                                sql_attr=sql_attr, is_mandatory=is_mandatory,
                                extraction_type=extraction_type,
                                source=source, is_foreign=is_foreign,
                                is_noop=is_noop)
    assert test_column.log_key == log_key
    assert test_column.is_json == is_json
    assert test_column.sql_attr == sql_attr
    assert test_column.is_mandatory == is_mandatory
    assert test_column.extraction_type == extraction_type
    assert test_column.is_foreign == is_foreign
    assert test_column.is_noop == is_noop


test_full_path_params = "source, is_foreign, extraction_type, log_key," \
                        "expected_value"


@pytest.mark.parametrize(test_full_path_params, [
    ('search', False, schema.ColumnType.EXTRACTION, 'actions',
     ['search', 'actions']),
    ('search', True, schema.ColumnType.EXTRACTION,
     'session_info.client', ['session_info', 'client']),
    ('search', False, schema.ColumnType.DERIVED, 'ctr', None),
    (None, False, schema.ColumnType.EXTRACTION, 'session_info',
     ['session_info']),
])
def test_full_path(source, is_foreign, extraction_type, log_key,
                   expected_value):
    test_column = schema.Column(source=source, is_foreign=is_foreign,
                                log_key=log_key,
                                extraction_type=extraction_type)
    assert test_column.full_path() == expected_value


def create_search(actions=None, results=None):
    return {'actions': actions, 'results': results}


test_extract_value_param = "source_type, source, is_foreign, log_key,\
                            is_mandatory, source_value, index, expected_value"


@pytest.mark.parametrize(test_extract_value_param, [
    (schema.SourceType.DICT, 'search', [], 'actions', True, 1, None, 1),
    (schema.SourceType.LIST, 'search.actions', [], 'test', True,
     [{'test': 1}], 0, 1),
    (schema.SourceType.LIST, 'search.actions', [], 'test', True,
     [None, {'test': 1}], 1, 1),
    (schema.SourceType.LIST, 'search', ['session_info'], 'session_info.agent',
     True, 1, None, 'iOS'),
    (schema.SourceType.DICT, 'search', [], 'missing', False, 1, None, None),
])
def test_extract_value(source_type, source, is_foreign, log_key,
                       is_mandatory, source_value, index, expected_value):
    name_to_object = {'search': create_search(actions=source_value),
                      'session_info': {'agent': 'iOS'}}
    column = schema.Column.create(source=source, log_key=log_key,
                                  is_mandatory=is_mandatory,
                                  source_type=source_type,
                                  is_foreign=is_foreign)
    assert column.extract_value(name_to_object,
                                index=index) == expected_value


@pytest.mark.parametrize('a_value, expected_value', [
    ('b', '{"a": "b"}'),
    (''.join(['b' for i in xrange(65535)]), None),
])
def test_extract_value_on_json(a_value, expected_value):
    name_to_object = {'search': {'blob': {'a': a_value}}}
    column = schema.Column.create(source='search', log_key='blob',
                                  source_type=schema.SourceType.DICT,
                                  is_json=True)
    assert column.extract_value(name_to_object)\
        == expected_value


def test_extract_value_on_timestamp():
    name_to_object = {'search': {'start_time': 1}}
    column = schema.Column.create(source='search', log_key='start_time',
                                  source_type=schema.SourceType.DICT,
                                  sql_attr='TIMESTAMP not null')
    assert column.extract_value(name_to_object) == 1000


def test_extract_value_on_derive_metric():
    column = schema.Column.create(source='search', is_derived=True,
                                  source_type=schema.SourceType.DICT)
    derive_metric = lambda name_to_object, metric_name, context: 1
    assert column.extract_value({},
                                derive_metric=derive_metric) == 1


def test_extract_value_on_missing_value():
    with pytest.raises(schema.RequiredFieldMissingException):
        column = schema.Column.create(source='search', log_key='a.b',
                                      is_mandatory=True,
                                      source_type=schema.SourceType.DICT)
        column.extract_value({'search': {'b': None}})


def create_result_table(log_key, expected_value):
    source = 'search.results'
    source_type = schema.SourceType.LIST
    source_value = [{'business_id': 1}, {'business_id': 2}]
    column = schema.Column.create(source=source,
                                  source_type=source_type,
                                  log_key=log_key)
    table = schema.Table(source=source,
                         source_type=source_type,
                         columns=[column])
    return table, source_value, expected_value


def create_search_table(log_key, expected_value):
    source = 'search'
    source_type = schema.SourceType.DICT
    source_value = [{'business_id': 1}, {'business_id': 2}]
    column = schema.Column.create(source=source,
                                  source_type=source_type,
                                  log_key=log_key)
    primary_key_column = schema.Column.create(source=source,
                                              source_type=source_type,
                                              is_noop=True,
                                              name='primary_key')
    table = schema.Table(source=source,
                         source_type=source_type,
                         columns=[column, primary_key_column])
    return table, source_value, expected_value


test_table_value_iterator_param = "table, source_value, expected_value"


@pytest.mark.parametrize(test_table_value_iterator_param, [
    create_result_table('index', [{'index': 0}, {'index': 1}]),
    create_result_table('business_id', [{'business_id': 1},
                                        {'business_id': 2}]),
    create_search_table('results', [{'results': [{'business_id': 1},
                                                 {'business_id': 2}]}]),
])
def test_table_value_iterator(table, source_value, expected_value):
    name_to_object = {'search': create_search(results=source_value)}
    result_under_test = [result for result in
                         table.value_iterator(name_to_object=name_to_object)]
    assert result_under_test == expected_value


@pytest.mark.parametrize("string, expected_value", [
    ('list', schema.SourceType.LIST),
    ('dict', schema.SourceType.DICT),
    ('sparse_list', schema.SourceType.SPARSE_LIST),
])
def test_decode_from_string(string, expected_value):
    assert schema.SourceType.decode_from_string(string) == expected_value


def test_decode_from_string_error():
    with pytest.raises(ValueError):
        schema.SourceType.decode_from_string('a')


def test_create_table():
    json_dict = {'src': 'search', 'src_type': 'dict'}
    columns = object()
    table = schema.Table.create(json_dict, columns)
    assert table.source == 'search'
    assert table.source_type == schema.SourceType.DICT
    assert table.columns == columns


def test_table_value_iterator_error():
    with pytest.raises(ValueError):
        table = schema.Table.create({'src': 'search',
                                     'src_type': 'dict'}, object())
        table.source_type = 'bad type'
        [i for i in table.value_iterator({})]


def test_column_repr():
    log_key = 'search.a.b'
    column = schema.Column(log_key=log_key)
    assert repr(column) == repr(log_key)


def test_create_from_table():
    table = {'src': 'search', 'src_type': 'dict'}
    column = schema.Column.create_from_table(table, {})
    assert column.source == 'search'
    assert column.source_type == schema.SourceType.DICT


class FakeLogLine(object):
    @classmethod
    def create(cls):
        '''create FakeLogLine instance for testing'''
        instance = cls()
        instance.a = {'b': 1}
        return instance


class Datum(object):
    def __init__(self, session):
        self.session = session


test_get_deep_params = "x, path_list, default_value, expected_value"


@pytest.mark.parametrize(test_get_deep_params, [
    ({'a': {'b': 1}}, 'a.b'.split('.'), None, 1),
    ({'a': {'c': 1}}, 'a.b'.split('.'), None, None),
    (FakeLogLine.create(), 'a.b'.split('.'), None, 1),
    ({'a': {'b': None}}, 'a.b.c'.split('.'), object(), None),
    (Datum(session={'a': 5}), ['session', 'a'], None, 5),
])
def test_get_deep(x, path_list, default_value, expected_value):
    assert schema.get_deep(x, path_list, default_value) == expected_value
