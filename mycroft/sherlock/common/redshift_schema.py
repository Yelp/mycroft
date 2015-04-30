#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Interface to manipulate schema definition
that turns log into a RedShift database

"""

# TODO: hide schema internals from external callers


class RedShiftLogSchemaException(Exception):
    pass


class RedShiftLogSchema():
    def __init__(self, schema=None):
        self._schema = schema or dict()
        if schema is not None and schema.get('version') is None:
            # old style
            self._schema = dict([('tables', schema)])
            self._schema['version'] = max([0] + [
                schema[tbl].pop('version', 1) for tbl in schema.keys()
            ])

        self._schema.setdefault('version', 1)
        self._schema.setdefault('tables', {})

        # enforce new style sortkeys
        for table in self._schema['tables']:
            table_dict = self._schema['tables'][table]
            if 'sortkey_attr' in table_dict and\
               isinstance(table_dict['sortkey_attr'], str):
                table_dict['sortkey_attr'] = [table_dict['sortkey_attr']]
            self.table_enforce_new_sortkeys(table_dict)

        self.schema_validate(self._schema)

    def get_error_table(self):
        return 'pipeline_errors', {
            'columns': [
                {'log_key': 'starttime',    'sql_attr': 'TIMESTAMP'},
                {'log_key': 'filename',     'sql_attr': 'varchar(4096)'},
                {'log_key': 'line_number',  'sql_attr': 'int8'},
                {'log_key': 'error_reason', 'sql_attr': 'varchar(4096)'},
                {'log_key': 'error_detail', 'sql_attr': 'varchar(65535)'},
                {'log_key': 'raw_line',     'sql_attr': 'varchar(65535)'},
            ],
            'sortkey_attr': ['starttime'],
            'src': '',
            'src_type': 'dict'
        }

    def table_enforce_new_sortkeys(self, current_table):
        if 'sortkey_attr' not in current_table:
            sortkeys = []
            for col in current_table['columns']:
                if ' sortkey' in col['sql_attr']:
                    name = col['name'] if 'name' in col else col['log_key']
                    sortkeys.append(name)
                    col['sql_attr'] = col['sql_attr'].replace(' sortkey', '')
            current_table['sortkey_attr'] = sortkeys

    def get_table(self, tablename):
        return self.tables().get(tablename, None)

    def tables(self):
        return self._schema['tables']

    def schema(self):
        return self._schema

    def table_rename(self, cur_name, new_name):
        """ Rename table

        cur_name: current table name
        new_name: new table name
        """
        if self.tables().get(cur_name) is None:
            raise RedShiftLogSchemaException("table does not exist")
        self.tables()[new_name] = self.tables().pop(cur_name)

    def table_create(self, name, source, source_type, sortkeys=None,
                     add_source_filename=False):
        """ Create table definition

        name:           table name
        source:         a root element in source to which all source fields
                        in this table are relative to.
        source_type:    <list|sparse_list|dict>
            Hint on how to treat the source.  If a list, add index column
            to identify relative position in the source.  if sparse_list
            treat the source of type dict as a list with index, key, value
            as columns.
        sortkeys:       a list of sortkeys
        add_source_filename: If True, add derived column __source_filename__
            which allows one to indentify the source of data in each table row
        """
        if self.tables().get(name) is not None:
            raise RedShiftLogSchemaException("table already exists")

        if sortkeys is None:
            sortkeys = []

        table = {
            'sortkey_attr': sortkeys,
            'src_type': source_type,
            'src': source,
        }

        columns = []
        if source_type in ['list', 'sparse_list']:
            columns.append({
                'log_key': 'index',
                'sql_attr': 'smallint not null',
                'is_mandatory': True,
            })
        if add_source_filename:
            columns.append({
                'is_derived': True,
                'name': '__source_filename__',
                'sql_attr': 'varchar(4096)',
            })

        table['columns'] = columns
        self.tables()[name] = table

    def table_add(self, name, content):
        """ Adds table to existing schema

        name:       table name
        content:    table's contents as a list of columns
        """
        if self.tables().get(name) is not None:
            raise RedShiftLogSchemaException("table already exists")

        self.table_validate(content)
        self.tables()[name] = content

    def table_delete(self, name):
        """ Delete table definition

        name:           table name
        :returns:       deleted table
        """
        if self.tables().get(name) is None:
            raise RedShiftLogSchemaException("table does not exist")
        table = self.tables()[name]
        del self.tables()[name]
        return table

    def column_add(self, table, name, sql_attr,
                   log_key=None, is_json=False, is_foreign=False,
                   is_noop=False):
        """ Add column to table

        table:      table name
        name:       column name
        sql_attr:   sql attributes
        log_key:    key relative to 'source' where to find data
        is_json:    Treat data as JSON blog if True
        is_foreign: 'log_key' is not relative to 'source'
        is_noop:    No extraction or computation will be performed if its true
        """
        if self.tables().get(table) is None:
            raise RedShiftLogSchemaException("table does not exist")

        columns = self.tables()[table]['columns']
        names = set([c.get('name') or c.get('log_key') for c in columns])

        if name in names:
            raise RedShiftLogSchemaException("column already exists")

        sql_attr_lc = sql_attr.lower()  # to lowercase for ease of validation

        # if sortkey is specified in the column, remove it & add the column
        # to the list of sortkeys
        if 'sortkey' in sql_attr_lc:
            raise RedShiftLogSchemaException(
                "sortkeys must be added via sortkeys_add option"
            )

        col = {'sql_attr': sql_attr}

        if is_json:
            if sql_attr_lc.find('varchar') < 0:
                raise RedShiftLogSchemaException(
                    "JSON column must be 'varchar' attribute"
                )
            col['is_json'] = is_json

        if sql_attr_lc.find('not null') >= 0:
            col['is_mandatory'] = True

        if is_foreign is True:
            if log_key is None:
                raise RedShiftLogSchemaException(
                    "'log_key' must be set for foreign column"
                )
            col['is_foreign'] = True

        if log_key is not None:
            col['log_key'] = log_key
        elif is_noop:
            col['is_noop'] = True
        else:
            col['is_derived'] = True

        if name != log_key:
            col['name'] = name

        columns.append(col)

    def column_remove(self, table, name):
        """ Remove column from table

        table:      table name
        name:       column name
        :returns:   deleted column
        """
        if self.tables().get(table) is None:
            raise RedShiftLogSchemaException("table does not exist")

        columns = self.tables()[table]['columns']
        sortkeys = self.tables()[table]['sortkey_attr'] or []
        for idx in xrange(0, len(columns)):
            if columns[idx].get('name') == name or\
               columns[idx].get('log_key') == name:
                if name in sortkeys:
                    sortkeys.remove(name)
                column = columns[idx]
                del columns[idx]
                return column
        raise RedShiftLogSchemaException("column does not exist")

    def sortkeys_add(self, table, sortkeys):
        """ Add sortkeys to table

        table:      table name
        sortkeys:   list of sortkeys
        :returns:   None
        """
        current_table = self.tables().get(table)
        if current_table is None:
            raise RedShiftLogSchemaException("table does not exist")

        columns = current_table['columns']
        names = set([c.get('name') or c.get('log_key') for c in columns])
        sortkey_set = set(sortkeys)
        if not sortkey_set.issubset(names):
            raise RedShiftLogSchemaException("invalid sortkey column")
        current_table['sortkey_attr'] = sortkeys

    def version_get(self):
        return self._schema['version']

    def version_set(self, version):
        self._schema['version'] = version

    @staticmethod
    def schema_validate(schema):
        """ validate entire schema """
        for key in ['version', 'tables']:
            if key not in schema:
                raise RedShiftLogSchemaException(
                    "missing key '{0}'".format(key)
                )

        if isinstance(schema['tables'], dict) is False:
            raise RedShiftLogSchemaException(
                "Invalid type for 'tables' field"
            )

        for name in schema['tables']:
            RedShiftLogSchema().table_validate(schema['tables'][name])

    @staticmethod
    def table_validate(table):
        """ validate table contents """

        for key in ['src', 'src_type', 'columns', 'sortkey_attr']:
            if key not in table:
                raise RedShiftLogSchemaException(
                    "missing key '{0}'".format(key)
                )

        if table['src_type'] not in ['list', 'sparse_list', 'dict']:
            raise RedShiftLogSchemaException(
                "unknown value={0} for key 'src_type'".format(
                    table['src_type']
                )
            )

        if isinstance(table['columns'], list) is False:
            raise RedShiftLogSchemaException(
                "Invalid type for 'columns' field"
            )

        for column in table['columns']:
            RedShiftLogSchema.column_validate(column)

    @staticmethod
    def column_validate(column):
        """ validate column contents """
        for key in ['sql_attr']:
            if key not in column:
                raise RedShiftLogSchemaException(
                    "missing key '{0}'".format(key)
                )

        for key in ['is_foreign', 'is_mandatory', 'is_noop', 'is_json', 'is_derived']:
            if key in column and isinstance(column[key], bool) is False:
                raise RedShiftLogSchemaException(
                    "Invalid type={0} for column field".format(key)
                )

        if not len([key for key in ['log_key', 'name'] if key in column]):
                raise RedShiftLogSchemaException(
                    "Must have either 'log_key' or 'name' field"
                )

    @staticmethod
    def header():
        return '''# FORMAT
#
# <table_name_1>:
#   src: <path.to.log.field>   # root key
#   src_type: <list|dict|sparse_list>
#   columns:
#   - log_key: <key_1>        # relative to root key
#     sql_attr: <attr>        # type, sortkey, distkey,, etc
#     name: <name>            # OPTIONAL: column name if different from log_key
#     is_json: <true>         # OPTIONAL: hint to store column as JSON
#     is_mandatory: <true>    # OPTIONAL: hint to prohibit null values
#     is_derived: <true>      # OPTIONAL: derive field from row data
#                                 'name' must be present, 'log_key' is ignored
#   - log_key: <foreign_key>  # foreign key, absolute path
#     sql_attr:  <attr>
#     is_foreign: <true>
#   - log_key: index          # OPTIONAL: list index (sparselist too)
#     sql_attr: smallint
#   sortkey_attr:             # sortkey(s) for this table
#   - <name>                  # column name
#<table_name_2>:
#
# EXPECTED JSON ROW FORMAT
# if src_type = list:
#     { index: <idx>, value: src[<idx>], foreign_key_1: foreign_key_1, ... }
# if src_type = dict:
#     { dict, foreign_key_1: foreign_key_1, ... }
# if src_type = sparse_list:
#     { index: <key>, value: src[<key>], foreign_key_1: foreign_key_1, ... }
'''
