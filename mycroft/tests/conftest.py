# -*- coding: utf-8 -*-
import pytest
import boto


@pytest.yield_fixture(scope="session")
def setup_boto_config(request):
    """ Copy prior aws access and secret keys from boto config. Override them.
    Add a finalizer to reset these overrides back to initial values.
    """
    prev_access_key = boto.config.get('Credentials', 'aws_access_key_id', None)
    prev_secret_key = boto.config.get(
        'Credentials', 'aws_secret_access_key', None
    )
    section_added = False
    if not boto.config.has_section('Credentials'):
        section_added = True
        boto.config.add_section('Credentials')
    boto.config.set('Credentials', 'aws_access_key_id', 'anaccesskey')
    boto.config.set('Credentials', 'aws_secret_access_key', 'asecretkey')

    yield

    if section_added:
        boto.config.remove_section('Credentials')
    else:
        boto.config.set(
            'Credentials', 'aws_access_key_id', prev_access_key
        )
        boto.config.set(
            'Credentials', 'aws_secret_access_key', prev_secret_key
        )
