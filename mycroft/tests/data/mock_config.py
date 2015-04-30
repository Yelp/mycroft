# -*- coding: utf-8 -*-
MOCK_CONFIG = {
    'aws_config': {
        'scheduled_jobs_table': 'My_Scheduled_Jobs_Table',
        'region': 'us-west-2',
        'redshift_clusters': 'Redshift_Clusters_Table'
    },
    'sqs': {
        'num_messages_per_fetch': 2,
        'wait_time_secs': 20,
        'et_queue_name': 'et',
        'load_queue_name': 'load',
        'et_scanner_queue_name': 'et_scanner',
        'load_scanner_queue_name': 'load_scanner',
    },
    'run_local': {
        'logdir': 'logs',
        'session_file': 'tests/data/fakecredsfile.txt'
    },
    'scanner': {
        'worker_keepalive_sec': 300,
    },
    'max_error_retries': 1,
    'default_mrjob': 'some-job',
    'redshift_region_uri': 'us-west-2',
    'log_stream_name': 'mycroft-test',
    's3_mrjob_output_path_format': 's3://bucket/key/{0}/{1}/{2}',
    's3_schema_location_format': 's3://bucket/{0}/{1}'
}
