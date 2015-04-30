#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Convert aws sts get-session-token output into
one that is used by the instance profile
metadata server.
"""


import simplejson
import sys


def convert(sts_format_dict):

    cred_dict = sts_format_dict['Credentials']
    aws_key = cred_dict['AccessKeyId']
    aws_secret = cred_dict['SecretAccessKey']
    aws_token = cred_dict['SessionToken']
    aws_token_expiry = cred_dict['Expiration']
    return simplejson.dumps({
        'Code': 'Success',
        'LastUpdated': 'FiXME',
        'Type': 'AWS_HMAC',
        'AccessKeyId': aws_key,
        'SecretAccessKey': aws_secret,
        'Token': aws_token,
        'Expiration': aws_token_expiry,
    })


if __name__ == '__main__':
    print convert(simplejson.load(sys.stdin))
