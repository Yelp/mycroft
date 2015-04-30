# -*- coding: utf-8 -*-
import avro.schema
import os
import pytest


def assert_good_schema(schema_object):
    '''Raises AssertError if given schema instance dose not fulfill the
    following conditions:

    * Have 'doc' in the record level
    * Each field must have 'doc'
    * Each doc must be longer than 0 characters after stripping whitespace.
    '''
    # Make sure the doc field exists
    assert schema_object.doc
    # Make sure we have non-whitespace doc field
    assert len(schema_object.doc.strip()) > 0
    for field in schema_object.fields:
        assert field.doc
        assert len(field.doc.strip()) > 0


def test_assert_good_schema():
    missing_top_level_doc_json_text = '''
{"name": "missing_top_level_doc",
 "type": "record",
 "fields": []}
    '''
    missing_field_level_doc_json_text = '''
{"name": "missing_top_level_doc",
 "type": "record",
 "doc": "test_record",
 "fields": [
    {"name": "color",
     "type": "string"}
 ]}
    '''
    whitespace_top_level_doc_json_text = '''
{"name": "missing_top_level_doc",
 "type": "record",
 "doc": " ",
 "fields": []}
    '''
    whitespace_field_level_doc_json_text = '''
{"name": "missing_top_level_doc",
 "type": "record",
 "doc": "test_record",
 "fields": [
    {"name": "color",
     "type": "string",
     "doc": " "}
 ]}
    '''

    example_json_texts = [
        missing_top_level_doc_json_text,
        missing_field_level_doc_json_text,
        whitespace_top_level_doc_json_text,
        whitespace_field_level_doc_json_text,
    ]
    for json_text in example_json_texts:
        schema_obj = avro.schema.parse(json_text)
        with pytest.raises(AssertionError):
            assert_good_schema(schema_obj)


@pytest.fixture
def schema_path(request):
    return request.param


def pytest_generate_tests(metafunc):
    if 'schema_path' in metafunc.fixturenames:
        avro_folder = os.path.join('mycroft', 'avro')
        schema_names = [name for name in os.listdir(avro_folder)
                        if name.endswith('json')]
        schema_paths = [os.path.join(avro_folder, schema_name) for schema_name in schema_names]
        assert len(schema_names) > 0
        metafunc.parametrize('schema_path', schema_paths, indirect=True)


def test_all_schemas(schema_path):
    with open(schema_path, 'r') as f:
        json_text = f.read()
    schema_obj = avro.schema.parse(json_text)
    assert_good_schema(schema_obj)
