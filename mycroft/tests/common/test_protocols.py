# -*- coding: utf-8 -*-
import pytest

from sherlock.common.protocols import RedshiftExportProtocol


@pytest.fixture
def redshift_export_encoder():
    return RedshiftExportProtocol()


@pytest.mark.parametrize("input_value, expected_value", [
    ('hello\nworld', 'hello\\nworld'),           # embedded \n
    ('hello\rworld', 'hello\\rworld'),           # embedded \r
    ('hello|world', 'hello\|world'),             # embedded |
    ('hello\\world', 'hello\\\\world'),          # embedded backslash
    # embedded backslash with newline
    ('hello\\\nworld', 'hello\\\\\\nworld'),
    # noop
    ("\x1e", "\x1e"),
])
def test_escape_value(input_value, expected_value, redshift_export_encoder):
    output_under_test = redshift_export_encoder.escape_value(input_value)
    assert output_under_test == expected_value


def test_write_row(redshift_export_encoder):
    output_map = {'a': 1, 'b': 'B', 'c': None}
    assert redshift_export_encoder.write(None, output_map) == '1|B|'
