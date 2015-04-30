# -*- coding: utf-8 -*-
'''
mycroft.models.abstract_records abstracts the implementation between an object
binding with its underlying persistance mechanism.

Records implements iterable protocol.
'''
from boto.dynamodb2.exceptions import ValidationException
from boto.dynamodb2.exceptions import ConditionalCheckFailedException
from boto.dynamodb2.exceptions import ItemNotFound


def _verify_kwargs(kwargs, known_keys):
    if len(kwargs) == 0:
        raise ValueError("No kwargs were passed")
    kwargs_key_set = set(kwargs.iterkeys())
    if not kwargs_key_set.issubset(known_keys):
        raise ValueError("Unknown keys: {0}".format(
            kwargs_key_set - known_keys))


MAX_PAGE_SIZE = 50


class Records(object):

    def __init__(self, persistence_object=None, avro_schema_object=None):
        '''
        Private API. Unstable. Use it at your own risk.

        :param persistence_object: The implementation of a persistence_object;
            for example a dynamo table instance
        :param avro_schema_object: An Avro schema object that describes what's
            allowed in each item
        :type avro_schema_object: Schema
        '''
        self._persistence_object = persistence_object
        self._avro_schema_object = avro_schema_object
        self._avro_fields = dict((field.name, field) for field in self._avro_schema_object.fields)
        self._known_keys = frozenset((field.name
                                      for field
                                      in self._avro_schema_object.fields))

    def get(self, **kwargs):
        '''
        Returns an Record

        :param kwargs: all kwarg are used together to get the record. It's
            required to pass in at least one valid kwarg; an unknown kwarg, or
            no kwargs, will result in ValueError
        :returns: Record that matches given keys
        :rtype: :class:`.Record`
        :raises KeyError: if request record is not found
        :raises PrimaryKeyError: if request record does not have conforming
            primary key

        Example::
            >>> record = records.get(
                    hash_key='1:public:search search_results',
                    data_date='2014-07-26')
            >>> record.get(data_date=None, schema_name=None)
            {'data_date': '2014-07-26',
                    'schema_name': 'search_query_data_warehouse'}

            >>> # Example of no kwarg
            >>> records.get()
            ValueError
            >>> # Example of unknown kwarg
            >>> records.get(color='black')
            ValueError

        '''
        _verify_kwargs(kwargs, self._known_keys)

        try:
            item = self._persistence_object.get_item(
                consistent=True,
                **kwargs)
            return Record(item=item, known_keys=self._known_keys, avro_fields=self._avro_fields)
        except ValidationException as e:
            raise PrimaryKeyError(repr(e))
        except ItemNotFound as e:
            raise KeyError(repr(e))

    def put(self, **kwargs):
        '''
        Puts an Record

        :param kwargs: each kwarg becomes key/value pair in the record.
            Passing in unknown kwarg will result in ValueError. If item
            already exists, ValueError will be raised.
        :returns: True if Records successfully persist the record
        :rtype: boolean
        :raises ValueError: if unknown kwarg is given
        :raises ValueError: if a duplicate record already exists
        :raises PrimaryKeyError: if the given record does not have a
            conforming primary key

        Example::
            >>> success = records.put(
                    hash_key='1:public:search search_results',
                    data_date='2014-07-26', et_state='et_started')
            >>> success
            True
            >>> # Trying to put the same item again
            >>> success = records.put(
                    hash_key='1:public:search search_results',
                    data_date='2014-07-26', et_state='et_started')
            >>> ValueError

            >>> # Example of no kwarg
            >>> records.put()
            ValueError
            >>> # Example of unknown kwarg
            >>> records.put(color='black')
            ValueError
        '''
        _verify_kwargs(kwargs, self._known_keys)
        try:
            return self._persistence_object.put_item(kwargs)
        except ConditionalCheckFailedException as e:
            raise ValueError(repr(e))
        except ValidationException as e:
            raise PrimaryKeyError(repr(e))

    def query_by_index(self, index=None, **kwargs):
        '''
        Unstable API. Use at your own risk.

        :param index: name of index to be used for searching
        :type index: string
        :param **kwargs: each kwarg should be a component of the index
        :returns: An iterable of record
        :raises ValueError: if kwargs contains more than 2 components
        '''
        if len(kwargs) > 2:
            # For DynamoDB, an index does not have more than 2 component:
            # a hash key and a range key. So if it's more than 2, raise an
            # Error
            raise ValueError('index does not support more than 2 components: {0}'.format(kwargs))

        # When the needs arrives, we can add option to control what
        # type of query operator to use.
        filter_kwargs = dict(('{0}__eq'.format(key), value) for key, value in kwargs.iteritems()
                             if value is not None)
        result_set = self._persistence_object.query_2(
            limit=None,
            index=index,
            reverse=False,
            consistent=False,  # GSI does not support consistent read
            attributes=None,
            max_page_size=MAX_PAGE_SIZE,
            query_filter=None,
            conditional_operator=None,
            **filter_kwargs)

        def iter_record():
            for result in result_set:
                yield Record(
                    item=result, known_keys=self._known_keys, avro_fields=self._avro_fields
                )
        return iter_record()

    def batch_delete(self, items, **kwargs):
        '''
        delete desired items in a batch

        :param items: items to delete
        :type items: list
        :param **kwargs: each kwarg should be a component of the primary key
        :returns: None
        '''
        with self._persistence_object.batch_write() as batch:
            for item in items:
                primary_key_kwargs = item.get(**kwargs)
                batch.delete_item(**primary_key_kwargs)

    def __iter__(self):

        def iter_table():
            result_set = self._persistence_object.scan(
                limit=None,
                segment=None,
                total_segments=None,
                max_page_size=MAX_PAGE_SIZE,
                attributes=None,
                conditional_operator=None)
            for result in result_set:
                yield Record(
                    item=result, known_keys=self._known_keys, avro_fields=self._avro_fields
                )

        return iter_table()


class Record(object):

    def __init__(self, item=None, known_keys=None, avro_fields=None):
        '''
        Unstable API. Use at your own risk.

        :param item: the underlying storage that represents the record
        :type item: boto.dynamodb2.item.Item
        :param known_keys: set of known keys
        :type known_keys: frozenset
        '''
        self._item = item
        self._known_keys = known_keys
        self._avro_fields = avro_fields

    def update(self, **kwargs):
        '''
        Updates the record

        :returns: True if Record successfully update
        :rtype: boolean
        :raises ValueError: if there is no kwargs

        Example::
            >>> record.get(data_date=None, et_state='et_started')
            {'data_date': '2014-07-26', 'et_state': 'et_started'}
            >>> success = record.update(et_state='et_completed')
            >>> success
            True
            >>> record.get(data_date=None, et_state=None)
            {'data_date': '2014-07-26', 'et_state': 'et_completed'}

        '''
        _verify_kwargs(kwargs, self._known_keys)
        for key, value in kwargs.iteritems():
            self._item[key] = value
        return self._item.partial_save()

    def delete(self, user, reason):
        '''
        Deletes the record

        :param user: username who requests the deletion of the item
        :type user: string
        :param reason: a brief description on why the item is being indexed.
        :type reason: string
        :returns: True if Record is successfully deleted
        :type: boolean
        :raises ValueError: if user is None or a string with just whitespaces
        :raises ValueError: if reason is None or a string with just whitespaces

        Example::

            >>> success = record.delete('awssoa', 'archiving')
            >>> success
            True

        '''
        if user is None or '' == user.strip():
            raise ValueError('You need a valid user argument')
        if reason is None or '' == reason.strip():
            raise ValueError('You need a valid reason argument')
        return self._item.delete()

    def _get(self, key, default):
        if default is None:
            # override default from avro if it exists
            default = getattr(self._avro_fields[key], 'default', default)
        return self._item.get(key, default=default)

    def get(self, **kwargs):
        '''
        Returns a dictionary of key-value pair specified in the kwargs

        :param kwargs: each kwarg will be fetch from the record
        :returns: a dictionary
        :rtype: a dictionary
        :raises ValueError: if unknown kwarg is given

        Example::

            >>> record.get(data_date=None, run_id=None)
            {'data_date': '2014-07-24', 'run_id': '9413984uy31984u31894'}

            >>> # Example of no kwarg
            >>> record.get()
            ValueError
            >>> # Example of unknown kwarg
            >>> record.get(color='black')
            ValueError


        '''
        _verify_kwargs(kwargs, self._known_keys)
        return dict((key, self._get(key, default))
                    for key, default in kwargs.iteritems())


class PrimaryKeyError(Exception):
    '''This is raised when the given record does not have conforming
    primary key'''
    pass
