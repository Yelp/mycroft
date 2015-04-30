# -*- coding: utf-8 -*-
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_COMPLETE
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_RUNNING
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_SCHEDULED
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_PAUSED
from mycroft.models.scheduled_jobs import JOBS_ETL_STATUS_ERROR

# This record is inserted as part of the fixture
# of a local DynamoDB
SAMPLE_RECORD_IN_DB = {
    'hash_key': 'sample_hash_key',
}


# This record has not been inserted to local
# DynamoDB fixture
SAMPLE_RECORD_NOT_IN_DB = {
    'hash_key': '1',
}


SAMPLE_RECORD_MISSING_HASH_KEY = {
    's3_path': 'sample_s3_path',
}


SCHEDULED_JOB_INPUT_DICT = {
    'redshift_id': 'rs1',
    'log_name': 'user_test',
    'log_schema_version': 'third',
    'time_log_need_to_be_available': '48:00',
    'start_date': '2014-08-15',
    'end_date': None,
    's3_path': 's3://backet/kay',
    'uuid': 'c7ec27a02b9540b9b45b619adaed6e67',
    'contact_emails': set(["devnull@blah.com"]),
    'additional_arguments': '{"et_step": ["--force-et"]}'
}

SCHEDULED_JOB_WITH_FUTURE_START_DATE = {
    'et_status': 'null',
    'load_status': 'success',
    'et_status_last_updated_at': None,
    'load_status_last_updated_at': None,
    'et_last_successful_date': None,
    'load_last_successful_date': None,
    'time_log_need_to_be_available': '48:00',
    'start_date': '2113-07-01',  # Far into the future
    'end_date': None,
    'log_name': 'ad_click',
    'log_schema_version': 'initial',
    's3_path': 's3://us-west-1/ad_click_log',
    'contact_emails': set(['em1@blah.com', 'em2@blah.com']),
    'redshift_id': 'id_1',
    'uuid': '608ba413a16c4bc4a681b69be664b99b',
    'hash_key': 'hk4',
    'additional_arguments': '{"load_step": ["--force-load"]}'
}


SAMPLE_LOG_NAME = 'user_test'
SAMPLE_LOG_VERSION = 'initial'


SAMPLE_SCHEDULED_JOBS = [
    {
        'et_status': None,
        'load_status': None,
        'et_status_last_updated_at': None,
        'load_status_last_updated_at': None,
        'et_last_successful_date': None,
        'load_last_successful_date': None,
        'time_log_need_to_be_available': '24:00',
        'start_date': '2014-07-01',
        'end_date': None,
        'log_name': 'ad_click',
        'log_schema_version': 'initial',
        's3_path': 's3://us-west-1/ad_click_log',
        'contact_emails': set(['em1@blah.com', 'em2@blah.com']),
        'redshift_id': 'id_1',
        'uuid': '54ef563a7d444e809a69b7c81ce8245b',
        'hash_key': 'hk1',
        'additional_arguments': '{"load_step": ["--force-load"]}'},
    {
        'et_status': None,
        'load_status': None,
        'et_status_last_updated_at': None,
        'load_status_last_updated_at': None,
        'et_last_successful_date': None,
        'load_last_successful_date': None,
        'time_log_need_to_be_available': '24:00',
        'start_date': '2014-07-15',
        'end_date': None,
        'log_name': SAMPLE_LOG_NAME,
        'log_schema_version': SAMPLE_LOG_VERSION,
        's3_path': 's3://us-west-1/user_test_log',
        'contact_emails': set(['em3@blah.com', 'em4@blah.com']),
        'redshift_id': 'id_2',
        'uuid': 'c479987740d846fcbc2dbcec14517e47',
        'hash_key': 'hk2',
        'additional_arguments': '{"load_step": ["--force-load"]}'},
    {
        'et_status': None,
        'load_status': None,
        'et_status_last_updated_at': None,
        'load_status_last_updated_at': None,
        'et_last_successful_date': None,
        'load_last_successful_date': None,
        'time_log_need_to_be_available': '24:00',
        'start_date': '2014-07-15',
        'end_date': '2014-08-15',
        'log_name': SAMPLE_LOG_NAME,
        'log_schema_version': 'second',
        's3_path': 's3://us-west-1/user_test_log',
        'contact_emails': set(['em5@blah.com', 'em6@blah.com']),
        'redshift_id': 'id_3',
        'uuid': '29cf38084c27429eac0ff9ce2dfb6446',
        'hash_key': 'hk3',
        'additional_arguments': '{"et_step": ["--force-et"]}'}]


SAMPLE_LOG_NAME_RANGER = 'ranger'


SAMPLE_LOG_SCHEMA_VERSION_1 = 'my_test_ranger_1'


SAMPLE_LOG_SCHEMA_VERSION_2 = 'my_test_ranger_2'


SAMPLE_RECORD_ET_STATUS_RUNNING_1 = {
    'hash_key': '1',
    'et_status': JOBS_ETL_STATUS_RUNNING,
    'load_status': JOBS_ETL_STATUS_RUNNING,
    'log_name': SAMPLE_LOG_NAME_RANGER,
    'log_schema_version': SAMPLE_LOG_SCHEMA_VERSION_1,
    'uuid': '471c228bee8a484a9751e16a85814a18',
}

SAMPLE_RECORD_ET_STATUS_RUNNING_2 = {
    'hash_key': '2',
    'et_status': JOBS_ETL_STATUS_RUNNING,
    'log_name': SAMPLE_LOG_NAME_RANGER,
    'log_schema_version': SAMPLE_LOG_SCHEMA_VERSION_2,
    'uuid': '68886855fe6341a099d29c357d8f9887',
}

SAMPLE_RECORD_ET_STATUS_COMPLETE_3 = {
    'hash_key': '3',
    'et_status': JOBS_ETL_STATUS_COMPLETE,
    'uuid': '7991676fafcb4d8b80dd59277c68fd09'
}

SAMPLE_RECORD_ET_STATUS_SCHEDULED = {
    'hash_key': '4',
    'et_status': JOBS_ETL_STATUS_SCHEDULED,
    'load_status': JOBS_ETL_STATUS_SCHEDULED,
    'log_name': SAMPLE_LOG_NAME_RANGER,
    'log_schema_version': SAMPLE_LOG_SCHEMA_VERSION_1,
    'uuid': '9cd6708231d84d63a0b321c2dfc8572c',
}

SAMPLE_RECORD_ET_STATUS_PAUSED = {
    'hash_key': '5',
    'et_status': JOBS_ETL_STATUS_PAUSED,
    'load_status': JOBS_ETL_STATUS_PAUSED,
    'pause_requested': 1,
    'log_name': SAMPLE_LOG_NAME_RANGER,
    'log_schema_version': SAMPLE_LOG_SCHEMA_VERSION_1,
    'uuid': '3743339163514095912dfc357169b365',
}

SAMPLE_RECORD_ET_STATUS_ERROR = {
    'hash_key': '5',
    'et_status': JOBS_ETL_STATUS_ERROR,
    'load_status': JOBS_ETL_STATUS_ERROR,
    'log_name': SAMPLE_LOG_NAME_RANGER,
    'log_schema_version': SAMPLE_LOG_SCHEMA_VERSION_1,
    'uuid': '80705fde1f7b46ce9d3b384ca05eefff',
}

SCHEDULED_JOB_INPUT_DICT_CUSTOM_TIME_LOG_AVAIL = {
    'redshift_id': 'rs1',
    'log_name': 'user_test',
    'log_schema_version': 'third',
    'time_log_need_to_be_available': '48:00',
    'start_date': '2014-08-15',
    'end_date': None,
    's3_path': 's3://backet/kay',
    'uuid': 'c7ec27a02b9540b9b45b619adaed6e67',
    'contact_emails': set(["devnull@blah.com"]),
    'additional_arguments': '{"time_log_need_to_be_available": "12:30"}'
}
