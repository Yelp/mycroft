# -*- coding: utf-8 -*-
from mycroft.logic.aws_resources import check_dynamodb
from mycroft.logic.aws_resources import dynamodb_table_names
from mycroft.models.aws_connections import get_dynamodb_connection
from pyramid.view import view_config


@view_config(route_name='diagnostic', renderer='json_with_status')
def diagnostic(request):
    result = {}
    result.update(check_dynamodb(get_dynamodb_connection(),
                                 dynamodb_table_names()))
    status_code = 200 if result['dynamodb'] == 'pass' else 500
    return status_code, result
