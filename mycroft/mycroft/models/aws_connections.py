# -*- coding: utf-8 -*-
"""
This module is used to create the scheduled_job_object to be imported and
used where needed.  This creates something like a singleton in that we won't
need to create new connections to the dynamo db tables.
"""
import avro.schema  # noqa
import os
from boto.dynamodb2.table import Table

import boto.dynamodb2
import boto.dynamodb2.table
import boto.s3
import boto.sqs

from staticconf import read_string

from mycroft.models.etl_records import ETLRecords
from mycroft.models.redshift_clusters import RedshiftClusters
from mycroft.models.scheduled_jobs import ScheduledJobs
from mycroft.models.index_util import introspect_global_indexes
from mycroft.log_util import log_exception
from sherlock.common.aws import get_boto_creds


def get_avro_schema(path):
    abs_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), '../..', path)
    with open(abs_path, 'r') as f:
        json_text = f.read()
    return avro.schema.parse(json_text)


class TableConnection(object):

    _connection_dict = {}

    _TABLE_NAME_TO_PROPERTIES = {
        'ScheduledJobs': {
            'class': ScheduledJobs,
            'physical_id_key': 'aws_config.scheduled_jobs_table',
            'avro_schema': 'mycroft/avro/scheduled_jobs.json'
        },
        'ETLRecords': {
            'class': ETLRecords,
            'physical_id_key': 'aws_config.etl_records_table',
            'avro_schema': 'mycroft/avro/etl_record.json'
        },
        'RedshiftClusters': {
            'class': RedshiftClusters,
            'physical_id_key': 'aws_config.redshift_clusters',
            'avro_schema': 'mycroft/avro/redshift_cluster.json'
        },
    }

    _region_conn = None

    @classmethod
    def get_connection(cls, table_object_name):
        if table_object_name not in cls._connection_dict:
            if cls._region_conn is None:
                cls._region_conn = get_dynamodb_connection()
            table_properties = cls._TABLE_NAME_TO_PROPERTIES[table_object_name]
            avro_schema = get_avro_schema(table_properties['avro_schema'])
            table_name = read_string(table_properties['physical_id_key'])
            table = Table(
                table_name,
                connection=cls._region_conn
            )
            try:
                results = table.describe()
                raw_indexes = results['Table'].get('GlobalSecondaryIndexes', [])
                table.global_indexes = introspect_global_indexes(raw_indexes)
            except Exception:
                log_exception("Table Connection Failed")
            cls._connection_dict[table_object_name] = table_properties['class'](
                table,
                avro_schema
            )
        return cls._connection_dict[table_object_name]


def get_dynamodb_connection():
    '''
    :returns: dynamodb2 connection
    '''
    return boto.dynamodb2.connect_to_region(
        read_string('aws_config.region'),
        **get_boto_creds()
    )


def get_sqs_connection():
    '''
    :returns: sqs connection
    '''
    return boto.sqs.connect_to_region(
        read_string('aws_config.region'),
        **get_boto_creds()
    )


def get_s3_connection():
    '''
    :returns: s3 connection
    '''
    return boto.s3.connect_to_region(
        read_string('aws_config.region'),
        **get_boto_creds()
    )
