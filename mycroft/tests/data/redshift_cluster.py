# -*- coding: utf-8 -*-

# This record is inserted as part of the fixture
# of a local DynamoDB
SAMPLE_CLUSTER_IN_DB = {
    'redshift_id': '1',
    'port': 5439,
}


# This record has not been inserted to local
# DynamoDB fixture
SAMPLE_CLUSTER_NOT_IN_DB = {
    'redshift_id': '1',
    'port': 5555
}


SAMPLE_CLUSTER_MISSING_HASH_KEY = {
    'port': 4444
}


SAMPLE_CLUSTER_ITEMS = [
    {
        'redshift_id': 'cluster-1',
        'port': 5439,
        'host': 'cluster-1.account.regionredshift.amazonaws.com',
        'db_schema': 'PUBLIC'
    },
    {
        'redshift_id': 'cluster-2',
        'port': 5439,
        'host': 'cluster-2.account.regionredshift.amazonaws.com',
        'db_schema': 'PUBLIC'
    },
    {
        'redshift_id': 'cluster-3',
        'port': 5439,
        'host': 'cluster-3.account.regionredshift.amazonaws.com',
        'db_schema': 'PUBLIC'
    }
]

SAMPLE_REDSHIFT_ID = 'cluster-1'

REDSHIFT_CLUSTER_INPUT_DICT = {
    'redshift_id': 'cluster-4',
    'port': 5439,
    'host': 'cluster-4.account.regionredshift.amazonaws.com',
    'db_schema': 'PUBLIC',
    'groups': ['search_team', 'log_infra']
}
