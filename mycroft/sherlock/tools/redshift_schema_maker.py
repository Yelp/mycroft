#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Collection of classes to auto-guess object structure and create a taxonomy
that can be used to convert object fields into CSV columns and also to
generate redshift 'createtable' statements

Supported are arbitrary objects and proto encode definitions.

"""
import re
import simplejson
from sherlock.common.redshift_schema import RedShiftLogSchema


class TypeMismatch(Exception):
    pass


class TypeUnknown(Exception):
    pass


class RedShiftSchemaMaker():
    def __init__(self, exclude=None, prune=None):
        self.schema = {}
        self.exclude = [] if exclude is None else exclude
        self.prune = [] if prune is None else prune

    def set_schema_entry(self, key, val, is_foreign=False):
        """Update schema with leaf key,value pair from the input dict
        is_foreign: True for foreign keys only (generates foreign DB keys)
        """
        val_type = type(val).__name__
        val_len = len(simplejson.dumps(val))

        entry = self.schema.get(key, {
            'type': val_type, 'max_len': val_len
        })

        # check for type mismatch
        if entry['type'] != val_type:
            if entry['type'] == 'NoneType':
                # saw null first => optional
                entry['type'] = val_type
            elif entry['type'] == 'int' and val_type in ['float', 'long']:
                # auto promotion
                entry['type'] = val_type
            elif entry['type'] == 'long' and val_type == 'float':
                # auto promotion
                entry['type'] = val_type
            elif (entry['type'] in ['int', 'long', 'float'] and
                  val_type in ['int', 'long', 'float']):
                # avoid demotion
                pass
            elif ((entry['type'] in ['str', 'basestring'] and
                  val_type == 'unicode') or
                  (entry['type'] == 'unicode' and
                   val_type in ['str', 'basestring'])):
                entry['type'] = 'unicode'
            elif val_type != 'NoneType'and entry['type'] != 'NoneType':
                raise TypeMismatch("'{0}' type={1} does not match previously\
 observed {2}".format(key, val_type, entry['type']))

        if entry['max_len'] < val_len:
            entry['max_len'] = val_len
        entry['is_json'] = entry['type'] in ['dict', 'list']
        entry['is_foreign'] = is_foreign
        self.schema[key] = entry

    def process_key_value(self, key, value, depth):
        """Recursively iterate over value,
           set schema with leaf key,value pairs
        depth: depth to iterate, depth < 0 => until leaf node
        """
        if key in self.exclude:
            return

        if not depth or key in self.prune:
            return self.set_schema_entry(key, value)

        val_type = type(value)
        depth -= 1
        if val_type is list:
            self.set_schema_entry(key, value)
        elif val_type is dict:
            for val_key in value.keys():
                subkey = self.mk_key(key, val_key)
                self.process_key_value(subkey, value[val_key], depth)
        else:
            self.set_schema_entry(key, value)

    def process_object(self, obj, depth):
        """Process dict recursively up to max depth
        obj: a key,value dictionary with unlimited nesting
        depth: depth to iterate, depth < 0 => until leaf node
        """
        if obj is None:
            return

        if not depth:
            return self.set_schema_entry('value', obj)

        if type(obj) is dict:
            self.process_key_value('', obj, depth)
        else:
            self.set_schema_entry('value', obj)

    def process_foreign_object(self, name, obj):
        """Add foreign keys to the schema
        name: foreign key name
        obj: foreign object
        """
        if obj is None:
            return
        self.set_schema_entry(name, obj, True)

    @staticmethod
    def mk_key(parent, child):
        """Construct key path
        """
        return re.sub('^\.+', '', "{0}.{1}".format(
            parent, child.encode("utf-8"))
        )

    @staticmethod
    def type_to_sqlattr(val_type, val_len):
        if val_type in ['str', 'basestring', 'unicode']:
            return "varchar({0})".format(val_len)
        elif val_type in ['list', 'dict']:
            return "varchar(65535)"
        elif val_type == 'int':
            if val_len is None:         # unknown
                return 'int8'
            elif val_len <= 4:          # 0 - 9999
                return 'int2'
            elif val_len <= 9:          # 10000 - 999999999
                return 'int4'
            else:                       # 1000000000 - ...
                return 'int8'
        elif val_type == 'long':
            return 'int8'
        elif val_type == 'float':
            return 'float8'
        elif val_type == 'bool':
            return 'bool'
        elif val_type == 'NoneType':
            return None
        else:
            raise TypeUnknown("unknown type: {0}".format(val_type))

    @staticmethod
    def mk_table(db, table_name, src='', src_type='dict', sortkeys=None,
                 flattenNestedKeys=True, add_source_filename=False):
        """Create a table definition from db
        table_name: see RedshiftSchema.table_create
        src:        see RedshiftSchema.table_create
        src_type:   see RedshiftSchema.table_create
        sortkeys:   see RedshiftSchema.table_create
        flattenNestedKeys: Allow column names of the form
            'location.bounds.max' or change to location_bounds_max instead.
        add_source_filename:  see RedshiftSchema.table_create
        """
        rs_schema = RedShiftLogSchema()
        rs_schema.table_create(
            table_name,
            src,
            src_type,
            sortkeys,
            add_source_filename
        )
        columns = []

        # sort for consistent output
        for key in sorted(db.keys()):
            val = db[key]
            log_key = key
            sql_attr = RedShiftSchemaMaker.type_to_sqlattr(
                val['type'],
                val['max_len']
            )
            if sql_attr is None:
                continue

            is_json = val.get('is_json', False)
            is_foreign = val.get('is_foreign', False)
            # nested log_key are a pain in RedShift.
            # Ex. select results."location.bounds"
            # replace '.' with '_' instead.
            name = re.sub('\.', '_', key) if flattenNestedKeys is True else key

            if val.get('is_mandatory', False) is True:
                sql_attr += ' not null'

            if is_foreign:
                rs_schema.column_add(
                    table_name, name, sql_attr, log_key, is_json, is_foreign
                )
            else:
                columns.append(
                    [table_name, name, sql_attr, log_key, is_json, False]
                )
        for args in columns:
            rs_schema.column_add(*args)
        return rs_schema.tables()


class RedShiftProtoSchemaMaker():
    """Similar to RedShiftSchemaMaker defines a schema but
       uses proto encode definition instead.
    """
    def __init__(self, exclude=None):
        self.schema = {}
        self.exclude = [] if exclude is None else exclude

    def set_obj_def_optional(self, depth, obj, key):
        """Handle optional proto encode fields

        obj: current object
        key: a object attribute converted into an absolute key
                (i.e. search.location.parsed.accuracy)
        """
        if key in self.exclude:
            return

        obj_type = type(obj).__name__
        if obj_type == 'instance':
            self.set_obj_def(depth, obj.val_type, key, optional=True)
        else:
            self.set_obj_def_simple(obj, key, optional=True)

    def set_obj_def_simple(self, obj, key, optional):
        """Set schema entry in a format matching RedShiftSchemaMaker
        """
        obj_type = type(obj).__name__
        entry = {'max_len': 65535}

        if obj_type == 'type':
            entry['type'] = obj.__name__
        elif obj_type in ['dict_of', 'list_of']:
            entry['type'] = re.sub('_of$', '', obj_type)
        elif obj_type == 'enum':
            entry['type'] = 'str'
            # fill in max length among enum values
            sortfunc = lambda x: 0 if x is None else len(x)
            entry['max_len'] = len(max(obj.values, key=sortfunc))
        elif obj_type == 'constant':
            entry['type'] = type(obj.value).__name__
        elif obj_type == 'value':
            entry['type'] = obj.val_type.__name__
        else:
            raise TypeUnknown("unknown type: {0}".format(obj_type))

        entry['is_json'] = entry['type'] in ['dict', 'list']
        entry['is_mandatory'] = optional is False
        self.schema[key] = entry

    def set_obj_def(self, depth, obj, key, optional=False):
        """Recurively extract object attributes and their types.

        depth: depth to recurse, -1 => unlimited
        obj: current object
        key: a object attribute converted into an absolute key
                (i.e. search.location.parsed.accuracy)
        optional: True if proto encode field is optional
        """
        if key in self.exclude:
            return

        if not depth:
            return

        depth -= 1
        for attr_key in obj._attributes.keys():
            attr_val = obj._attributes[attr_key]
            attr_type = type(attr_val).__name__
            subkey = self.mk_key(key, attr_key)

            if attr_type == 'optional':
                self.set_obj_def_optional(depth, attr_val.val_type, subkey)
            elif attr_type == 'instance':
                self.set_obj_def(depth, attr_val.val_type, subkey, optional)
            else:
                self.set_obj_def_simple(attr_val, subkey, optional)

    @staticmethod
    def mk_key(parent, child):
        return RedShiftSchemaMaker().mk_key(parent, child)

    @staticmethod
    def mk_table(db, table_name, src,
                 src_type, flattenNestedKeys=True, add_source_filename=False):
        return RedShiftSchemaMaker().mk_table(
            db,
            table_name,
            src,
            src_type,
            flattenNestedKeys,
            add_source_filename,
        )
