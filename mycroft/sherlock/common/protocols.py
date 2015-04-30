# -*- coding: utf-8 -*-
""""
Contins useful protocol dealing with loading data into Redshift

RedshiftExportProtocol

    Data emitted from this protocol will use BACKSLASH to escape
    invalid characters. These invalid characteres includes:

        NEWLINE             \n
        CARRIAGE RETURN     \r
        PIPE                |
        BACKSLASH           \
"""
from cStringIO import StringIO

VALID_DELIMITERS = ['|', '\x1e']


class RedshiftExportProtocol(object):

    def __init__(self, output_order=None, delimiter='|'):
        self.given_output_order = output_order
        self.delimiter = delimiter.decode("string_escape")
        if self.delimiter not in VALID_DELIMITERS:
            raise ValueError("'{0}': invalid delimiter".format(self.delimiter))

    def _sorted_keys(self, keys):
        return sorted(keys)

    def write(self, _, row):
        row = dict(
            (k, v.encode('utf-8'))
            if isinstance(v, basestring) else (k, v)
            for k, v in row.iteritems())

        io = StringIO()

        output_order = self.given_output_order \
            if self.given_output_order else self._sorted_keys(row.keys())

        for key in output_order:
            output = row[key]
            if output is None:
                pass
            elif isinstance(output, basestring):
                output = self.escape_value(output)
            else:
                output = unicode(output).encode('utf-8')

            if output is not None:
                io.write(output)

            io.write(self.delimiter.encode('utf-8'))
        return io.getvalue()[:-1]

    def escape_value(self, value):
        return value.replace(
            '\\', '\\\\'
        ).replace(
            '\n', '\\n'
        ).replace(
            '\r', '\\r'
        ).replace(
            self.delimiter, '\\' + self.delimiter
        )
