# -*- coding: utf-8 -*-
'''
Common AWS function, includes fetching temporary credentials from instance
profile metadata.

You can test the functionality via the following command

inenv -- python sherlock/common/aws.py --config config.yaml

or on a deployed machine on AWS

run-service-batch sherlock python \
        sherlock/common/aws.py --config config.yaml


'''
import urllib2
from collections import namedtuple
import os

import argparse
from datetime import datetime
import simplejson
import staticconf

MAX_UNIX_TIME = "2038-01-01T00:00:00Z"


TempCredentials = namedtuple(
    'TempCredential',
    'last_updated, access_key_id, secret_access_key, token, expiration'
)


def _is_custom_aws_instance():
    return staticconf.read_bool('is_custom_aws_instance', False)


def get_boto_creds():
    creds_tuple = get_aws_creds(not _is_custom_aws_instance())
    boto_creds = {}
    boto_creds['aws_secret_access_key'] = creds_tuple.secret_access_key
    boto_creds['aws_access_key_id'] = creds_tuple.access_key_id
    boto_creds['security_token'] = creds_tuple.token
    return boto_creds


def get_aws_creds(from_local_file, check_if_valid=True):
    '''
    Returns a TempCredential instance based on the metadata server. You should
    not cache beyond the stated expiration timestamp. Metadata server will
    automatically populate the latest valid credential. You should call this
    function again to obtain valid credentials'''

    cred_dict = fetch_creds_from_file() if from_local_file else fetch_creds()

    creds = TempCredentials(
        last_updated=cred_dict['LastUpdated'],
        access_key_id=cred_dict['AccessKeyId'],
        secret_access_key=cred_dict['SecretAccessKey'],
        token=cred_dict['Token'],
        expiration=cred_dict['Expiration']
    )

    if check_if_valid is True:
        # check for expired credentials
        try:
            exp = datetime.strptime(creds.expiration, "%Y-%m-%dT%H:%M:%SZ")
        except:
            exp = datetime.strptime(creds.expiration, "%Y-%m-%dT%H:%M:%S.%fZ")
        now = datetime.utcnow()
        if exp < now:
            raise Exception("Expired credentials: {0}, now: {1}".format(
                creds.expiration, now
            ))
    return creds


def fetch_creds():
    '''
    Return a dictionary holding temporary credentials from the metadata server.
    This function will block upto the timeout specified in config file. You may
    not call this method unless config.yaml is loaded
    '''
    url = '{url_root}/{name}'.format(
        url_root=staticconf.read_string('instance_profile_creds_url'),
        name=staticconf.read_string('instance_profile_name'))
    in_stream = urllib2.urlopen(
        url,
        timeout=staticconf.read_int(
            'instance_profile_creds_timeout_in_seconds', default=4
        )
    )
    return simplejson.load(in_stream)


def fetch_creds_from_file():
    '''
    Returns a dictionary holding credentials from a file defined in config.yaml
    '''
    with open(staticconf.read_string('run_local.session_file'), 'r') as creds:
        if os.fstat(creds.fileno()).st_size == 0:
            raise Exception("session file is empty")
        creds_dict = simplejson.load(creds)
        creds_dict['Expiration'] = creds_dict.get('Expiration', MAX_UNIX_TIME)
        for optional_key in ['Token', 'LastUpdated']:
            creds_dict[optional_key] = creds_dict.get(optional_key)
        return creds_dict


def load_config(path):
    staticconf.YamlConfiguration(path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Print out current temp credentials'
    )
    parser.add_argument(
        '--config',
        help='config.yaml that contains instance profile name'
    )
    args = parser.parse_args()
    load_config(args.config)
    print get_aws_creds(False)
