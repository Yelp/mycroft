# -*- coding: utf-8 -*-
import pytest
import staticconf
import re

from boto.exception import S3ResponseError

from mycroft.logic.schema_actions import _get_log_versions_from_list
from mycroft.logic.schema_actions import _get_key_name
from mycroft.logic.schema_actions import list_versions_by_log_name
from mycroft.logic.schema_actions import list_all_log_versions
from mycroft.logic.schema_actions import get_key_url
from mycroft.logic.schema_actions import PATH_RE_PREFIX
from mycroft.logic.schema_actions import post_key
from tests.helpers.fake_s3 import FakeBucket
from tests.helpers.fake_s3 import FakeBucketWithBadConnection

LOG_NAMES = ['logname1', 'logname2']
VERSIONS = ['va', 'vb', 'vc']
BAD_KEYS = [
    'mycroft/stream/bad_log/schema/schema.yaml',
    'mycroft/stream/bad_log/schema/bad_version/my_folder/schema.yaml',
    'mycroft/stream/bad_log/foo/bad_version/schema.yaml'
    'mycroft/stream/bad_log/schema/bad_version/skeema.yaml'
    'mycroft/stream/bad_log//schema/bad_version/schema.yaml'
    'mycroft/stream/../schema/bad_version/schema.yaml'
    'mycroft/stream/bad_log/schema/../schema.yaml'
    ]


@pytest.fixture
def fake_bucket():
    fb = FakeBucket('fakebucket')
    key_path = "mycroft/stream/{0}/schema/{1}/schema.yaml"
    for lgn in LOG_NAMES:
        for version in VERSIONS:
            path = key_path.format(lgn, version)
            new_key = fb.new_key(path)
            new_key.set_contents_from_string("{0}_{1}".format(lgn, version))
    fb.force_consistent_list()
    return fb


@pytest.fixture
def fake_bucket_with_non_matching_keys():
    fb = FakeBucket('fakebucket_no_match')
    for key_path in BAD_KEYS:
        new_key = fb.new_key(key_path)
        new_key.set_contents_from_string(key_path)
    fb.force_consistent_list()
    return fb


def assert_equivalent_sequences(sequence_1, sequence_2):
    assert len(sequence_1) == len(sequence_2)
    assert set(sequence_1) == set(sequence_2)


def test_list_versions_with_no_match(fake_bucket_with_non_matching_keys):
    return_value = list_all_log_versions(fake_bucket_with_non_matching_keys)
    assert len(return_value['schemas']) == 0


def test_list_versions_by_log_name(fake_bucket):
    for input_log_name in LOG_NAMES:
        return_value = list_versions_by_log_name(fake_bucket, input_log_name)
        returned_log_version = assert_dict_has_correct_structure(return_value)
        assert returned_log_version == input_log_name


def assert_dict_has_correct_structure(test_dict):
    # correct structure is {'log_name': logname, 'versions': [v1, v2, ...]}
    # return the value associated with 'log_name' key to potentially use in
    # further assertions
    assert_equivalent_sequences(test_dict.keys(), ['log_name', 'versions'])
    assert_equivalent_sequences(test_dict['versions'], VERSIONS)
    return test_dict['log_name']


def test__get_log_versions_from_list(fake_bucket_with_non_matching_keys):
    return_value = _get_log_versions_from_list(
        fake_bucket_with_non_matching_keys, None)
    assert len(return_value) == 0


def test__get_key_name():
    log_name = 'x'
    log_version = 'y'
    return_value = _get_key_name(log_name, log_version)
    s3_log_prefix = staticconf.read_string('s3_log_prefix')
    path_re = re.compile(PATH_RE_PREFIX.format(s3_log_prefix))
    assert path_re.match(return_value) is not None


def test__get_key_name_1():
    log_name = ''
    log_version = 'y'
    with pytest.raises(ValueError):
        _get_key_name(log_name, log_version)


@pytest.mark.parametrize("log_name, log_version", [
    ('x', None), (None, 'y'), ('..', 'y'), ('x', '.'),
    ('x', ''), ('', 'y')])
def test__get_key_name_2(log_name, log_version):
    with pytest.raises(ValueError):
        _get_key_name(log_name, log_version)


def test_list_versions_by_unknown_log_name(fake_bucket):
    return_value = list_versions_by_log_name(fake_bucket, 'blah')
    expected_output = {'log_name': 'blah', 'versions': []}
    assert return_value == expected_output


def test_list_all_log_versions(fake_bucket):
    # Note -- the return_value here has the form:
    # {'schemas': [{'logname': logname1, 'versions': [v1,v2,v2]},
    #              {'logname': logname2, 'versions': [v1,v2,v2]}]}
    # because of the nesting & potential out of order lists of versions direct
    # comparison with all schemas isn't possible
    return_value = list_all_log_versions(fake_bucket)
    assert_equivalent_sequences(return_value.keys(), ['schemas'])
    returned_log_names = []
    for schema_dict in return_value['schemas']:
        lg_name = assert_dict_has_correct_structure(schema_dict)
        returned_log_names.append(lg_name)
    assert_equivalent_sequences(returned_log_names, LOG_NAMES)


def test_post_key(fake_bucket):
    contents = 'logname1_vd'
    return_value = post_key(fake_bucket, 'logname1', 'vd', contents)
    expected_result = {
        'log_name': 'logname1',
        'log_version': 'vd',
        'bytes_written': len(contents)}
    assert return_value == expected_result


def test_post_key_bad_connection():
    fb = FakeBucketWithBadConnection('fakebucket_no_match')
    with pytest.raises(S3ResponseError) as e:
        post_key(fb, 'badcon', 'badcon', 'contents')
    assert e.value.reason == "POST failed - incomplete"
    assert e.value.status == 502


def test_post_key_overwrite(fake_bucket):
    with pytest.raises(S3ResponseError) as e:
        post_key(fake_bucket, 'logname1', 'va', 'logname1_vzzz')
    assert e.value.reason == "POST failed - overwrite or connection failure"
    assert e.value.status == 500


def test_get_key_url(fake_bucket):
    return_value = get_key_url(fake_bucket, 'logname1', 'va')
    assert return_value is not None


def test_get_key_url_bad_keyname(fake_bucket):
    key_template = "fakebucket mycroft/stream/{0}/schema/{1}/schema.yaml"
    k_instance = key_template.format('logname0', 'va')
    with pytest.raises(S3ResponseError) as e:
        get_key_url(fake_bucket, 'logname0', 'va')
    assert e.value.status == 404
    assert e.value.reason == "{0} pair is not in s3".format(k_instance)
