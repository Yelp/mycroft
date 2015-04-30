# -*- coding: utf-8 -*-
from yaml import safe_load
from mycroft.models.index_util import introspect_global_indexes
from mycroft.models.index_util import introspect_schema
from boto.dynamodb2.fields import HashKey, RangeKey, GlobalAllIndex

import pytest


raw_indexes = \
    """
- IndexName: LoadStatusIndex
  IndexSizeBytes: 13545
  IndexStatus: ACTIVE
  ItemCount: 25
  KeySchema:
  - {AttributeName: load_status, KeyType: HASH}
  Projection: {ProjectionType: ALL}
  ProvisionedThroughput: {NumberOfDecreasesToday: 0, ReadCapacityUnits: 1, WriteCapacityUnits: 1}
- IndexName: LogNameLogSchemaVersionIndex
  IndexSizeBytes: 13545
  IndexStatus: ACTIVE
  ItemCount: 25
  KeySchema:
  - {AttributeName: log_name, KeyType: HASH}
  - {AttributeName: log_schema_version, KeyType: RANGE}
  Projection: {ProjectionType: ALL}
  ProvisionedThroughput: {NumberOfDecreasesToday: 0, ReadCapacityUnits: 1, WriteCapacityUnits: 1}
- IndexName: ETStatusIndex
  IndexSizeBytes: 13545
  IndexStatus: ACTIVE
  ItemCount: 25
  KeySchema:
  - {AttributeName: et_status, KeyType: HASH}
  Projection: {ProjectionType: ALL}
  ProvisionedThroughput: {NumberOfDecreasesToday: 0, ReadCapacityUnits: 1, WriteCapacityUnits: 1}
    """


def test_introspect_global_indexes():
    indexes = introspect_global_indexes(safe_load(raw_indexes))
    assert len(indexes) == 3
    for index in indexes:
        assert isinstance(index, GlobalAllIndex)


@pytest.mark.parametrize("raw_schema", [
    [{'AttributeName': 'load_status', 'KeyType': 'HASH'}],
    [{'AttributeName': 'log_name', 'KeyType': 'HASH'},
     {'AttributeName': 'log_schema_version', 'KeyType': 'RANGE'}],
    [{'AttributeName': 'et_status', 'KeyType': 'HASH'}],
])
def test_introspect_schema(raw_schema):
    schema = introspect_schema(raw_schema)
    assert len(schema) == len(raw_schema)
    for i in xrange(len(schema)):
        key_type = raw_schema[i]['KeyType']
        if key_type == 'HASH':
            assert isinstance(schema[i], HashKey)
        elif key_type == 'RANGE':
            assert isinstance(schema[i], RangeKey)
