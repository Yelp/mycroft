# -*- coding: utf-8 -*-
from pyramid.view import view_config
from mycroft.models.aws_connections import get_dynamodb_connection
from mycroft.models.aws_connections import get_sqs_connection
from mycroft.models.aws_connections import get_s3_connection


_NAME_TO_FUNC = {
    'dynamodb': get_dynamodb_connection,
    'sqs': get_sqs_connection,
    's3': get_s3_connection
}


@view_config(route_name='status.healthcheck', renderer='json_with_status')
def healthcheck(request):
    status_code = 200

    results = dict((k, 'fail' if fn() is None else 'pass') for k, fn in _NAME_TO_FUNC.iteritems())

    status_code = 200 if all(results[k] == 'pass' for k in results) else 500

    return status_code, results
