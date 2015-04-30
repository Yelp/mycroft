# -*- coding: utf-8 -*-
"""
**config**
==========

A collection of functions to support initialization of the web app
"""

import logging
import os
import simplejson
import yaml
from pyramid.httpexceptions import HTTPFound

from sherlock.common.config_util import load_default_config
from mycroft.logic import schema_actions

log = logging.getLogger('mycroft.config')


def routes(config):

    # diagnostic endpoints
    config.add_route('diagnostic', '/v1/diagnostic')

    # schema endpoints
    config.add_route(
        'api.schema_log_name_and_version',
        '/v1/schema/{log_name}/{version}')
    config.add_route('api.schema_log_name', '/v1/schema/{log_name}')
    config.add_route('api.schema', '/v1/schema')

    # jobs endpoints
    config.add_route(
        'api.jobs_log_name_version',
        '/v1/jobs/filtered/{log_name}/{log_schema_version}')
    config.add_route('api.jobs_log_name', '/v1/jobs/filtered/{log_name}')
    config.add_route('api.jobs', '/v1/jobs')
    config.add_route('api.jobs_job_id', '/v1/jobs/job')

    # runs endpoints
    config.add_route('api.runs_job_id', '/v1/runs/{job_id}')

    # clusters endpoints
    config.add_route('api.clusters_cluster_name', '/v1/clusters/{cluster_name}')
    config.add_route('api.clusters', '/v1/clusters')

    # log_source endpoints
    config.add_route('api.log_source', '/v1/log_source')
    config.add_route('api.log_source.search', '/v1/log_source/search')

    # swagger documentation endpoints
    config.add_route('swagger', '/api-docs')
    config.add_route('swagger.schema', '/api-docs/schema')
    config.add_route('swagger.jobs', '/api-docs/jobs')
    config.add_route('swagger.runs', '/api-docs/runs')
    config.add_route('swagger.clusters', '/api-docs/clusters')
    config.add_route('swagger.log_source', '/api-docs/log_source')

    # Serve the dashboard from /web/
    config.add_static_view(name='web', path='static/html/')
    config.add_static_view(name='static', path='static/')

    # Add a root url hanlder
    config.add_route('root_redirect', '')

    def handler(request):
        raise HTTPFound('/web/')
    config.add_view(
        handler,
        route_name='root_redirect'
    )


def swagger_views(config):
    """Registers swagger views with config. This happens programmatically
    as we need to introspect over all the defined views

    :param config: the pyramid Configurator for this webapp
    :type config: pyramid.config.Configurator

    """
    for endpoint in ['schema', 'jobs', 'runs', 'clusters', 'log_source']:
        filename = 'api-docs/{0}.yaml'.format(endpoint)
        routename = 'swagger.{0}'.format(endpoint)

        def handler(request, filename=filename):
            with open(filename, 'r') as f:
                file_contents = f.read()
            return yaml.load(file_contents)

        config.add_view(handler, route_name=routename, renderer='json')


def json_with_status_renderer_factory(info):
    """Renderer for the common case of JSON with a status code.

    The format which needs to be returned is (int, dict) with the first
    parameter being a HTTP status code and the second paramter being the JSON
    data to serialize.

    .. deprecated:: 0.2 (2013-08-01)

    .. warning::

        This function is now deprecated. It can be used to help migrate from
        Tornado to Pyramid. Please use :class:`pyramid.renderer.JSON`
        with ``renderer='json'`` and use :mod:`pyramid.httpexceptions` to
        return non-200 response codes.
    """
    def _render(value, system):
        if not isinstance(value, tuple):
            raise TypeError('renderer was passed non-tuple as value')
        if len(value) != 2:
            raise ValueError('renderer expected a tuple of length 2 as value '
                             '(got tuple of length %d)' % len(value))

        status, data = value

        if not isinstance(data, dict):
            raise TypeError('data must be a dict, not %r' % type(data))

        request = system['request']
        response = request.response
        response.content_type = "application/json"
        response.status_int = status
        return simplejson.dumps(data)
    return _render


def renderers(config):
    config.add_renderer('json_with_status', factory='mycroft.config:'
                        'json_with_status_renderer_factory')


def load_staticconf(config_path='config.yaml', env_config_path='config-env-dev.yaml'):
    """
    This function loads the static configurations; Note the input arguments
    merely provide defaults if the SERVICE_CONFIG_PATH and/or
    SERVICE_ENV_CONFIG_PATH environment variables are not set.

    :param config_path: path to the config
    :type config_path: string

    :param env_config_path: path to the environment dependent config
    :type env_config_path: string

    """
    SERVICE_CONFIG_PATH = os.environ.get('SERVICE_CONFIG_PATH', config_path)
    SERVICE_ENV_CONFIG_PATH = os.environ.get('SERVICE_ENV_CONFIG_PATH',
                                             env_config_path)
    load_default_config(SERVICE_CONFIG_PATH, SERVICE_ENV_CONFIG_PATH)


def init():
    schema_actions.init()
