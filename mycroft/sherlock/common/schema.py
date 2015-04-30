# -*- coding: utf-8 -*-
'''schema.py contains code to extract values according to schema'''
import re
import simplejson


class ColumnType(object):
    '''
    Enumeration for different types of columns

    EXTRACTION  : value is extracted from source
    DERIVED     : value is derived (separate calculation must be done)
    '''

    EXTRACTION = 0
    DERIVED = 1


class SourceType(object):
    '''
    Enumeration for different type of projection

    LIST        : source is a list and individual list element is projected as
                  a separate row
    DICT        : source is a dict-like and the dict is projected as one row
    SPARSE_LIST : Unknown yet
    '''

    LIST = 0
    DICT = 1
    SPARSE_LIST = 2

    name_to_type = {
        'list': LIST,
        'dict': DICT,
        'sparse_list': SPARSE_LIST,
    }

    @classmethod
    def decode_from_string(cls, type_string):
        if type_string in cls.name_to_type:
            return cls.name_to_type[type_string]
        raise ValueError("unknown type: {name}".format(name=type_string))


class RequiredFieldMissingException(Exception):
    '''A required field is missing'''
    pass


# A sentinel object to denote a missing value
MISSING_VALUE = object()


class Table(object):
    '''Represents individual table in Redshift'''

    def __init__(self, source=None, source_type=None, columns=None):
        self.source = source
        self.source_type = source_type
        self.columns = columns
        self.truncated_columns = {}

    @classmethod
    def create(cls, json_dict, columns):
        return cls(
            source=json_dict.get('src'),
            source_type=SourceType.decode_from_string(
                json_dict.get('src_type')
            ),
            columns=columns
        )

    @classmethod
    def get_value_char_width(cls, val):
        return len(val if type(val) in [str, unicode] else "{0}".format(val))

    def check_for_truncation(self, column, value):
        if column.is_char and value is not None and \
                self.get_value_char_width(value) > column.char_width:
            if not self.truncated_columns.get(column.name):
                self.truncated_columns[column.name] = 0
            self.truncated_columns[column.name] += 1
        return value

    def value_iterator(self, name_to_object, derive_metric=None):
        if self.source_type == SourceType.LIST:
            log_value = \
                get_deep(name_to_object, self.source.split('.'), MISSING_VALUE)
            for index in xrange(len(log_value)):
                row = dict(
                    (column.name,
                     self.check_for_truncation(
                         column,
                         column.extract_value(name_to_object,
                                              index=index,
                                              derive_metric=derive_metric)
                         ))
                    for column in self.columns if not column.is_noop)
                yield row
        elif self.source_type == SourceType.DICT:
            yield dict((column.name,
                        self.check_for_truncation(
                            column,
                            column.extract_value(name_to_object,
                                                 index=None,
                                                 derive_metric=derive_metric)
                        ))
                       for column in self.columns if not column.is_noop)
        else:
            raise ValueError("unknown type")


class Position(int):
    '''Represents a position of an list element

    For example, for the following object:

    {'results': [{'a': 'A'}, {'b': 'B'}]}

    To reference the 'A' value, we use the following path list:

    ['results', Position(0), 'a']
    '''
    pass


class PositionLiteral(int):
    '''Represents a position literal of an list element

    For example, for the following object:

    {'results': [{'a': 'A'}, {'b': 'B'}]}

    To reference the index 0 of the list, we use the following path list:

    ['results', Position(0), PositionLiteral(0)]
    '''

    pass


class Column(object):

    def __init__(self, log_key=None, name=None, sql_attr=None, is_json=False,
                 is_mandatory=False, extraction_type=None, source=None,
                 source_type=None, is_foreign=False, is_noop=False):
        self.log_key = log_key
        self._name = name
        self.sql_attr = sql_attr
        self.is_json = is_json
        self.is_mandatory = is_mandatory
        self.extraction_type = extraction_type
        self.source = source
        self.source_type = source_type
        self.is_foreign = is_foreign
        self.is_noop = is_noop
        self.is_char = False
        self.char_width = 0
        if sql_attr:
            char_match = re.search('(?:var)?char\((\d+)\)', sql_attr, flags=re.IGNORECASE)
            if char_match:
                self.is_char = True
                self.char_width = int(char_match.groups()[0])

    def __repr__(self):
        return repr(self.log_key)

    @classmethod
    def create(cls, **kwargs):
        return cls(log_key=kwargs.get('log_key'),
                   name=kwargs.get('name'),
                   sql_attr=kwargs.get('sql_attr'),
                   is_json=kwargs.get('is_json'),
                   is_mandatory=kwargs.get('is_mandatory'),
                   extraction_type=kwargs.get('is_derived'),
                   source=kwargs.get('source'),
                   is_foreign=kwargs.get('is_foreign'),
                   source_type=kwargs.get('source_type'),
                   is_noop=kwargs.get('is_noop'),)

    @classmethod
    def create_from_table(cls, table, column_map):
        info = dict(column_map)
        info['source'] = table['src']
        info['source_type'] = SourceType.decode_from_string(
            table.get('src_type')
        )
        return cls.create(**info)

    def full_path(self, env=None):
        if env is None:
            env = {}
        if self.extraction_type == ColumnType.DERIVED:
            return None
        elif self.is_foreign:
            return self.log_key.split('.')
        elif self.source is None or self.source == '':
            return self.log_key.split('.')
        elif self.source_type == SourceType.LIST:
            if self.log_key == 'index':
                return self.source.split('.') + [
                    PositionLiteral(env['position'])
                ]
            else:
                return self.source.split('.') + [
                    Position(env['position'])
                ] + self.log_key.split('.')
        else:
            return self.source.split('.') + self.log_key.split('.')

    @property
    def name(self):
        return self._name if self._name else self.log_key

    @property
    def is_derived(self):
        return self.extraction_type == ColumnType.DERIVED

    def extract_value(self, document, index=None, derive_metric=None):

        if self.is_derived is True:
            return derive_metric(
                name_to_object=document,
                metric_name=self.name,
                context={'self': self, 'index': index},
            )

        path_list = self.full_path({'position': index})
        log_value = get_deep(document, path_list, MISSING_VALUE)

        if log_value is MISSING_VALUE:
            if self.is_mandatory is True:
                raise RequiredFieldMissingException('Required key: {path} is \
                        missing'.format(path=path_list))
            else:
                log_value = None

        # dealing with json encoding
        if self.is_json is True:
            log_value = simplejson.dumps(log_value)

            # TODO: Better strategy to deal with data too long to fit a column
            # For now, we are going to drop the row
            # dealing with max length
            if len(log_value) > 65535:
                # apparently 65535 limit on json on redshift column
                log_value = None

        # TODO: Make this data driven instead of breaking data abstraction
        # We shouldn't be hard coding start_time to fix epoch into epochmillis
        if self.sql_attr is not None and self.sql_attr.startswith('TIMESTAMP'):
            if log_value is not None:
                log_value = long(log_value) * 1000

        return log_value


def get_deep(x, path_list, default_value):
    '''Returns the element from x according to the given path_list

    x can be type dict or other object. Element from path_list is the direction
    to locate the desirable attribute.

    Path element can be a simple basestring. For example

    For x: {'search': {'agent': 'iOS'}}, path_list = ['search', 'agent'] will
    return iOS

    Path element can also be complex type.

    Args:
        x - A map or an object
        path_list - a list of path
        default_value - default value to be returned if path does not locate
                        the element.

    Returns:
        the element
    '''
    for path_element in path_list:
        if x is None:
            return x
        elif isinstance(path_element, Position):
            x = x[path_element]
        elif isinstance(path_element, PositionLiteral):
            x = path_element
        elif isinstance(x, dict):
            x = x.get(path_element, default_value)
        else:
            x = getattr(x, path_element, default_value)

        if x is default_value:
            return x
    return x
