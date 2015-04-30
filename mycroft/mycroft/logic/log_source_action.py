# -*- coding: utf-8 -*-
"""
**logic.log_source_actions**
========================

A collection of functions to handle actions relating to source logs
in the mycroft service.  The C part of MVC for mycroft/log_source
"""
import requests
import staticconf


def search_log_source_by_keyword(request_body):
    disabled_logfinder = staticconf.read_bool('disable_logfinder_service')
    if disabled_logfinder:
        return {'logs': []}

    # send HTTP request
    search_endpoint = staticconf.read_string('log_finder_search_end_point')
    response = requests.post(search_endpoint, request_body)

    # if we get a bad HTTP status, raise an exception
    response.raise_for_status()

    content = response.json()
    return content


def get_log_meta_data(bucket_name, log_name):
    if bucket_name is None or log_name is None:
        return None

    if staticconf.read_bool('disable_logfinder_service'):
        return None

    # send HTTP request
    endpoint = staticconf.read_string('log_finder_buckets_end_point') \
        + '/' + bucket_name + '/' + log_name
    response = requests.get(endpoint)

    # if we get a bad HTTP status, raise an exception
    response.raise_for_status()

    return response.json()
