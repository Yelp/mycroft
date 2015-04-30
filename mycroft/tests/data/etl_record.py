# -*- coding: utf-8 -*-

# This record is inserted as part of the fixture
# of a local DynamoDB
SAMPLE_RECORD_IN_DB = {
    'hash_key': '1',
    'data_date': 'sample_date'
}


# This record has not been inserted to local
# DynamoDB fixture
SAMPLE_RECORD_NOT_IN_DB = {
    'hash_key': '1',
    'data_date': 'some_random_date'
}


SAMPLE_RECORD_MISSING_HASH_KEY = {
    'data_date': 'some_random_date'
}


SAMPLE_RECORD_MISSING_RANGE_KEY = {
    'hash_key': '2'
}


SAMPLE_RECORD_JOBS = [
    {
        'hash_key': '1a',
        'data_date': '2014-07-01',
        'etl_status': 'load_complete',
        'job_id': '7a024c5416f24c42aa6d70190eb0fdf2',
    },
    {
        'hash_key': '2a',
        'data_date': '2014-07-02',
        'etl_status': 'et_started',
        'job_id': '7a024c5416f24c42aa6d70190eb0fdf2',
    },
    {
        'hash_key': '3a',
        'data_date': '2014-07-01',
        'etl_status': 'load_error',
        'job_id': '54ef563a7d444e809a69b7c81ce8245b',
    }
]

SAMPLE_JOB_ID = '7a024c5416f24c42aa6d70190eb0fdf2'
