# -*- coding: utf-8 -*-
import mock
import pytest
import staticconf.testing
import urllib2

from sherlock.common import aws


MOCK_CREDS = {
    "Code": "Success",
    "LastUpdated": "2014-03-12T17:17:07Z",
    "Type": "AWS-HMAC",
    "AccessKeyId": "My Access Key Id",
    "SecretAccessKey": "My Secret Access Key",
    "Token": "My Token",
    "Expiration": "2014-03-12T23:47:56Z",
}


@pytest.fixture
def generate_mock_creds():
    return dict(MOCK_CREDS)


def test_get_temp_creds_from_instance_profile(generate_mock_creds):
    config = {
        'use_instance_profile': True
    }
    with staticconf.testing.MockConfiguration(config):
        with mock.patch('sherlock.common.aws.fetch_creds',
                        autospec=True) as mock_creds:
            mock_creds.return_value = MOCK_CREDS
            temp_cred = aws.get_aws_creds(False, check_if_valid=False)
            assert temp_cred.last_updated == MOCK_CREDS['LastUpdated']
            assert temp_cred.access_key_id == MOCK_CREDS['AccessKeyId']
            assert temp_cred.secret_access_key == MOCK_CREDS['SecretAccessKey']
            assert temp_cred.token == MOCK_CREDS['Token']
            assert temp_cred.expiration == MOCK_CREDS['Expiration']


def test_fetch_creds_timeout():
    config = {
        'instance_profile_name': 'sherlock',
        'instance_profile_creds_url': 'http://169.254.169.254/latest',
        'instance_profile_creds_timeout_in_seconds': 0.1,
    }
    with staticconf.testing.MockConfiguration(config):
        with pytest.raises(urllib2.URLError):
            aws.fetch_creds()
