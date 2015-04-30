# -*- coding: utf-8 -*-
from boto.dynamodb2 import exceptions
from boto.dynamodb2.fields import (HashKey, RangeKey,
                                   GlobalAllIndex, GlobalKeysOnlyIndex,
                                   GlobalIncludeIndex)
from boto.dynamodb2.types import STRING


def introspect_global_indexes(raw_indexes):
    """
    Given a raw index structure back from a DynamoDB response, parse
    out & build the high-level Python objects that represent them.
    """
    indexes = []

    for field in raw_indexes:
        index_klass = GlobalAllIndex
        kwargs = {
            'parts': []
        }
        projection_type = field['Projection']['ProjectionType']
        type_names = ['ALL', 'KEYS_ONLY', 'INCLUDE']
        type_classes = [GlobalAllIndex, GlobalKeysOnlyIndex, GlobalIncludeIndex]
        type_name_to_class = dict(zip(type_names, type_classes))
        try:
            index_klass = type_name_to_class[projection_type]
        except KeyError:
            raise exceptions.UnknownIndexFieldError(
                "%s was seen, but is unknown. Please report this at "
                "https://github.com/boto/boto/issues." %
                projection_type
            )
        if projection_type == 'INCLUDE':
            kwargs['includes'] = field['Projection']['NonKeyAttributes']
        name = field['IndexName']
        kwargs['parts'] = introspect_schema(field['KeySchema'], None)
        indexes.append(index_klass(name, **kwargs))

    return indexes


def introspect_schema(raw_schema, raw_attributes=None):
    """
    Given a raw schema structure back from a DynamoDB response, parse
    out & build the high-level Python objects that represent them.
    """
    schema = []
    sane_attributes = {}

    if raw_attributes:
        for field in raw_attributes:
            sane_attributes[field['AttributeName']] = field['AttributeType']
    key_name_to_class = {'HASH': HashKey, 'RANGE': RangeKey}
    for field in raw_schema:
        data_type = sane_attributes.get(field['AttributeName'], STRING)
        key_type_name = field['KeyType']
        try:
            schema.append(key_name_to_class[key_type_name](field['AttributeName'],
                                                           data_type=data_type))
        except KeyError:
            raise exceptions.UnknownSchemaFieldError(
                "%s was seen, but is unknown. Please report this at "
                "https://github.com/boto/boto/issues." % key_type_name
            )
    return schema
