# -*- coding: utf-8 -*-
from datetime import datetime
from datetime import timedelta
import mock
import os
import pytest

from staticconf import YamlConfiguration

from sherlock.common.redshift_psql import get_namespaced_tablename


@pytest.fixture(scope="module", params=[False, True])
def pgsql(request):
    YamlConfiguration('tests/batch/dummy_config.yaml')
    from sherlock.common.redshift_psql import RedshiftPostgres
    logstream = 'dummy_stream'
    psql_auth_file = 'tests/batch/dummy_user_file.txt'
    with mock.patch('sherlock.common.aws.fetch_creds',
                    autospec=True) as fetch_creds:
        fetch_creds.return_value = {
            "Code": "Success",
            "LastUpdated": "2014-03-12T17:17:07Z",
            "Type": "AWS-HMAC",
            "AccessKeyId": "My Access Key Id",
            "SecretAccessKey": "My Secret Access Key",
            "Token": "My Token",
            "Expiration": (
                datetime.utcnow() + timedelta(seconds=60)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        run_local = request.param
        if run_local is True:
            # local file contains expired creds, test assertion code
            with pytest.raises(Exception):
                RedshiftPostgres(logstream, psql_auth_file, run_local)
        return RedshiftPostgres(logstream, psql_auth_file, run_local=False)


@pytest.mark.parametrize("input_value, expected_value", [
    ("copy bad_stuff", "copy cleansed "),
    ("select good_stuff", "select good_stuff"),
    ("copy good_stuff", "copy cleansed "),
    ("select bad_stuff", "select bad_stuff"),
])
def test_cleanse_sql(input_value, expected_value, pgsql):
    output_under_test = pgsql.cleanse_sql(input_value)
    assert output_under_test == expected_value


@pytest.mark.parametrize("input_config, expected_out", [
    ("rs_schema_testy.yaml", "testy.table_name_blah"),
    ("rs_schema_public.yaml", "table_name_blah"),
    ("rs_schema_PUBLIC.yaml", "table_name_blah"),
    ("rs_schema_none.yaml", "table_name_blah"),
])
def test_get_namespaced_tablename_config(input_config, expected_out):
    filepath = os.path.join('tests', 'common', input_config)
    YamlConfiguration(filepath)
    output_under_test = get_namespaced_tablename("table_name_blah")
    assert output_under_test == expected_out


@pytest.mark.parametrize("input_schema, expected_out", [
    ("testy", "testy.table_name_blah"),
    ("public", "table_name_blah"),
    ("PUBLIC", "table_name_blah"),
])
def test_get_namespaced_tablename_arg(input_schema, expected_out):
    output_under_test = get_namespaced_tablename(
        "table_name_blah",
        schemaname=input_schema
    )
    assert output_under_test == expected_out
