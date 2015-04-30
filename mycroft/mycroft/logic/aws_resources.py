# -*- coding: utf-8 -*-
'''
STOP. Think before using this module. This is specific access to AWS and you
shouldn't be using it directly in mycroft. There should be proper abstraction
in place for rest of mycroft components without directly accessing this module.
'''
import staticconf


def dynamodb_table_names():
    '''
    :returns: iterable of string that each element is a DyanmoDB table name used in mycroft
    '''
    table_names = []
    # append other table resources required by mycroft
    table_names.append(staticconf.read_string('aws_config.scheduled_jobs_table'))
    return table_names


def check_dynamodb(connection, table_names):
    '''
    :returns: a dictinoary with the following keys:
        dynamodb - either 'pass' or 'fail'
        dynamodb_exception - optional string of repr(exception)
    '''
    try:
        [connection.describe_table(table_name) for table_name in table_names]
        return {'dynamodb': 'pass'}
    except Exception as e:
        return {'dynamodb': 'fail', 'dynamodb_exception': repr(e)}
