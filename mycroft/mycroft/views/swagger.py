# -*- coding: utf-8 -*-
"""Serves /swagger endpoint."""
import yaml
from pyramid.view import view_config


@view_config(route_name='swagger', request_method='GET', renderer='json')
def apidocs(request):
    with open('api-docs/swagger.yaml', 'r') as f:
        swagger_content = f.read()
    return yaml.load(swagger_content)
