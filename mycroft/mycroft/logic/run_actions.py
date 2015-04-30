# -*- coding: utf-8 -*-
"""
**logic.run_actions**
========================

A collection of functions to handle actions relating to runs
in the mycroft service.  The C part of MVC for mycroft/runs
"""

import re

HEX_STRING = re.compile("[a-f0-9]+$")

ETL_RECORD_ARGS = {
    'hash_key': None,
    'data_date': None,
    'etl_status': None,
    'et_runtime': None,
    'et_starttime': None,
    'load_runtime': None,
    'load_starttime': None,
    'redshift_id': None,
    's3_path': None,
    'updated_at': None,
    'job_id': None,
    'run_by': None,
    'etl_error': None,
    'additional_arguments': None,
}


def _parse_runs(query_result):
    """
    _parse_runs converts the results of a query on the backing store
    to a dictionary with a key of 'runs' and value of a list of runs

    *Each run returned in the list is of the form*::

        {
          'hash_key': '<hash_key>',
          'job_id': '4c03c69b2b6c4cf8a1742fa55de02244',
          'data_date': '2014-05-01',
          'etl_status': 'et_complete',
          'et_runtime': '1140',
          'et_starttime': '2014-08-30T00:31:44.129398',
          'load_runtime': None,
          'load_starttime': None,
          'redshift_id': 'cluster',
          's3_path': 's3://bucket/key1/schema.yaml',
          'updated_at': '2014-08-30T00:31:44.129398',
          'run_by': 'mycroft_runner',
          'etl_error': None,
          'additional_arguments': '{"et_step": ["--force-et"]}'
        }

    Args:
    query_result -- iterable result of the query from the backing store

    Returns:
    A dict of {'runs': [run1, run2, ...]}
    """
    return {'runs': [etl_record.get(**ETL_RECORD_ARGS) for etl_record in query_result]}


def list_runs_by_job_id(job_id, etl_records_object):
    """
    lists all runs for a particular log_name

    *Each job returned in the list is of the form*::

        {
          'hash_key': '<hash_key>',
          'job_id': '4c03c69b2b6c4cf8a1742fa55de02244',
          'data_date': '2014-05-01',
          'etl_status': 'et_complete',
          'et_runtime': '1140',
          'et_starttime': '2014-08-30T00:31:44.129398',
          'load_runtime': None,
          'load_starttime': None,
          'redshift_id': 'cluster',
          's3_path': 's3://bucket/key1/schema.yaml',
          'updated_at': '2014-08-30T00:31:44.129398',
          'run_by': 'mycroft_runner',
          'etl_error': None,
          'additional_arguments': '{"et_step": ["--force-et"]}'
        }

    :param job_id: uuid of the job
    :type job_id: string of a hexadecimal number
    :param etl_records_object: the ETLRecords object from which we read jobs
    :type etl_records_object: an instance of ETLRecords

    :returns: a dict of {'runs': [run1, run2, ...]}
    :rtype: dict

    :raises ValueError: for invalid job_id
    """
    if job_id is None or HEX_STRING.match(job_id) is None:
        raise ValueError("invalid job_id {0}".format(job_id))

    filtered_runs = etl_records_object.get_runs_with_job_id(job_id)
    return _parse_runs(filtered_runs)
