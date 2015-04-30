# -*- coding: utf-8 -*-
'''
mycroft.models.redshift_clusters persists metadata regarding each redshift cluster.

RedshiftClusters is a collection of RedshiftCluster. RedshiftClusters implements iterable protocol.

RedshiftCluster contains metadata for a redshift cluster

Example::

    >>> redshift_clusters = RedshiftClusters(dynamo_table_object)
    >>> # Create
    >>> success = redshift_clusters.put(redshift_id='cluster',
            host='cluster.us-west-2.redshift.amazonaws.com',
            port=5439)
    >>> success
    True
    >>> # Read
    >>> redshift_cluster = redshift_clusters.get(redshift_id='cluster')
    >>> redshift_cluster.get(host=None)
    {'host': 'cluster.us-west-2.redshift.amazonaws.com'}
    >>> # Update
    >>> success = redshift_cluster.update(
            host='cluster.us-west-3.redshift.amazonaws.com')
    >>> success
    True
    >>> # Delete
    >>> redshift_cluster.delete(redshift_id='cluster', reason='Archiving')
    >>> # Iterate all clusters # it will be slow
    >>> for cluster in redshift_clusters:
          print cluster
    <RedshiftCluster #some_id>
    <RedshiftCluster #some_other_id>

'''

from mycroft.models.abstract_records import Records


class RedshiftClusters(object):

    def __init__(self, persistence_object=None, avro_schema_object=None):
        '''
        Private API. Unstable. Use it at your own risk.

        :param persistence_object: The implementation of a persistence_object;
            for example a dynamo table instance
        :param avro_schema_object: An Avro schema object that describes what's
            allowed in each RedshiftCluster
        :type avro_schema_object: Schema
        '''
        self._clusters = Records(
            persistence_object=persistence_object,
            avro_schema_object=avro_schema_object)

    def get(self, **kwargs):
        '''
        Returns an RedshiftCluster

        :param kwargs: all kwarg are used together to get the cluster. It's
            required to pass in at least one valid kwarg; an unknown kwarg, or
            no kwargs, will result in ValueError
        :returns: RedshiftCluster that matches given keys
        :rtype: :class:`.RedshiftCluster`
        :raises KeyError: if request cluster is not found
        :raises PrimaryKeyError: if request cluster does not have conforming
            primary key

        Example::
            >>> redshift_cluster = redshift_clusters.get(redshift_id='cluster')
            >>> redshift_cluster.get(host=None, port=None)
            {'host': 'cluster.us-west-3.redshift.amazonaws.com',
                     'port': 5439}

            >>> # Example of no kwarg
            >>> redshift_clusters.get()
            ValueError
            >>> # Example of unknown kwarg
            >>> redshift_clusters.get(color='black')
            ValueError

        '''
        return RedshiftCluster(self._clusters.get(**kwargs))

    def put(self, **kwargs):
        '''
        Puts a RedshiftCluster

        :param kwargs: each kwarg becomes key/value pair in the cluster.
            Passing in unknown kwarg will result in ValueError. If item
            already exists, ValueError will be raised.
        :returns: True if RedshiftClusters successfully persist the cluster
        :rtype: boolean
        :raises ValueError: if unknown kwarg is given
        :raises ValueError: if a duplicate cluster already exists
        :raises PrimaryKeyError: if the given cluster does not have a
            conforming primary key

        Example::
            >>> success = redshift_clusters.put(redshift_id='cluster',
                    host='cluster.us-west-2.redshift.amazonaws.com',
                    port=5439)
            >>> success
            True
            >>> # Trying to put the same item again
            >>> success = redshift_clusters.put(redshift_id='cluster',
                    host='cluster.us-west-2.redshift.amazonaws.com',
                    port=5439)
            >>> ValueError

            >>> # Example of no kwarg
            >>> redshift_clusters.put()
            ValueError
            >>> # Example of unknown kwarg
            >>> redshift_clusters.put(color='black')
            ValueError
        '''
        return self._clusters.put(**kwargs)

    def __iter__(self):

        def iter_clusters():
            for cluster in self._clusters:
                yield RedshiftCluster(cluster=cluster)

        return iter_clusters()


class RedshiftCluster(object):

    def __init__(self, cluster=None):
        '''
        Unstable API. Use at your own risk.

        :param cluster: the underlying storage that represents the cluster
        :type cluster: mycroft.models.abstract_records.Record
        '''
        self._cluster = cluster

    def update(self, **kwargs):
        '''
        Updates the cluster

        :returns: True if RedshiftCluster successfully update
        :rtype: boolean
        :raises ValueError: if there is no kwargs

        Example::
            >>> redshift_cluster.get(port=None)
            {'port': 5439}
            >>> success = redshift_cluster.update(port=5555)
            >>> success
            True
            >>> redshift_cluster.get(port=None)
            {'port': 5555}

        '''
        return self._cluster.update(**kwargs)

    def delete(self, user, reason):
        '''
        Deletes the cluster

        :param user: username who requests the deletion of the item
        :type user: string
        :param reason: a brief description on why the item is being indexed.
        :type reason: string
        :returns: True if RedshiftCluster is successfully deleted
        :type: boolean
        :raises ValueError: if user is None or a string with just whitespaces
        :raises ValueError: if reason is None or a string with just whitespaces

        Example::

            >>> success = redshift_cluster.delete('awssoa', 'archiving')
            >>> success
            True

        '''
        return self._cluster.delete(user, reason)

    def get(self, **kwargs):
        '''
        Returns a dictionary of key-value pair specified in the kwargs

        :param kwargs: each kwarg will be fetch from the cluster
        :returns: a dictionary
        :rtype: a dictionary
        :raises ValueError: if unknown kwarg is given

        Example::

            >>> redshift_cluster.get(host=None, port=None)
            {'host': 'cluster.us-west-2.redshift.amazonaws.com',
             'port': 5439}

            >>> # Example of no kwarg
            >>> redshift_cluster.get()
            ValueError
            >>> # Example of unknown kwarg
            >>> redshift_cluster.get(color='black')
            ValueError


        '''
        return self._cluster.get(**kwargs)
