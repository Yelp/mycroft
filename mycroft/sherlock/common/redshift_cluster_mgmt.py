#!/usr/bin/env python
# -*- coding: utf-8 -*-

from datetime import datetime
import time
import sys
from boto.redshift.layer1 import RedshiftConnection
from boto.redshift.exceptions import ClusterNotFound
from sherlock.common.util import get_deep


class RedshiftClusterMgmt():
    """
    Higher-level interface to manage databases.  For more information
    please visit the `aws redshift documentation index
    <http://docs.aws.amazon.com/cli/latest/reference/redshift/index.html>`_
    """

    def __init__(self, connection=None, **kw_args):
        self.poll_interval_sec = kw_args.pop('poll_interval_sec', 30)
        if connection is None:
            self._conn = RedshiftConnection(**kw_args)
        else:
            self._conn = connection

    def get_cluster_parameters(self, parameter_group_name):
        """
        gets the result of a DescribeClusterParameters for a particular
        parameter group.

        :param parameter_group_name: redshift cluster parameter group name
        :type paramenter_group_name: string

        :returns: the parameters for the cluster paramenter_group_name
        :rtype: a dictionary of parameters
        """
        response = self._conn.describe_cluster_parameters(parameter_group_name)
        result_dict = get_deep(
            response,
            'DescribeClusterParametersResponse.DescribeClusterParametersResult'
        )
        parameters = result_dict['Parameters']
        marker = result_dict['Marker']
        if marker is not None:
            # If marker is not None, that means we have more than 100
            # clusters, and we need to pagniated to fetch more results
            raise NotImplementedError
        return parameters

    def get_cluster_list(self, cluster_name=None):
        """
        gets a list of cluster descrptions; if a cluster_name is specified
        gets the specified clusters' description

        :param cluster_name: optional name of cluster
        :type cluster_name: string

        :returns: the parameters for the cluster specified or list of cluster descriptions
        :rtype: a list of cluster descriptions (which are dicts)
        """
        response = self._conn.describe_clusters(cluster_name)
        result_dict = get_deep(
            response,
            'DescribeClustersResponse.DescribeClustersResult'
        )
        cluster_list = result_dict['Clusters']
        marker = result_dict['Marker']
        if marker is not None:
            # If marker is not None, that means we have more than 100
            # clusters, and we need to pagniated to fetch more results
            raise NotImplementedError
        return cluster_list

    def get_cluster_info(self, cluster_name):
        """
        gets the specified clusters' description

        :param cluster_name: name of cluster
        :type cluster_name: string

        :returns: the parameters for the cluster specified
        :rtype: a dictionary description of a cluster
        """
        cluster_list = self.get_cluster_list(cluster_name)
        if len(cluster_list) > 1:
            raise ValueError("{0}: too many clusters".format(cluster_name))
        return cluster_list.pop()

    def get_cluster_status(self, cluster_name):
        """
        gets the specified clusters' status.  Possible values include:

        * available
        * creating
        * deleting
        * rebooting
        * renaming
        * resizing

        :param cluster_name: name of cluster
        :type cluster_name: string

        :returns: the status of the cluster
        :rtype: string
        """
        cluster = self.get_cluster_info(cluster_name)
        return cluster['ClusterStatus']

    def get_cluster_and_restore_status(self, cluster_name):
        """
        gets the specified clusters' status and restore status.  Useful in
        polling as a cluster is restored from a snapshot.

        Possible cluster status values include:

        * available
        * creating
        * deleting
        * rebooting
        * renaming
        * resizing

        Possbile cluster restore status values include:

        * starting
        * restoring
        * completed
        * failed

        :param cluster_name: name of cluster
        :type cluster_name: string

        :returns: a pair of cluster status and restore status
        :rtype: string
        """
        cluster = self.get_cluster_info(cluster_name)
        return "{0},{1}".format(
            get_deep(cluster, 'ClusterStatus'),
            get_deep(cluster, 'RestoreStatus.Status')
        )

    def get_cluster_and_pg_status(self, cluster_name):
        """
        gets the specified clusters' status and parameter group status.  Useful
        in polling as a cluster's parameters are changed.

        Possible cluster status values include:

        * available
        * creating
        * deleting
        * rebooting
        * renaming
        * resizing

        Possbile cluster parameter group apply status values include:

        * in-sync
        * pending-reboot

        :param cluster_name: name of cluster
        :type cluster_name: string

        :returns: a pair of cluster status and parameter group apply status
        :rtype: string
        """
        cluster = self.get_cluster_info(cluster_name)
        return "{0},{1}".format(
            cluster['ClusterStatus'],
            cluster['ClusterParameterGroups'][0]['ParameterApplyStatus'],
        )

    def set_vpc_security_group_ids(self, cluster_name, vpc_security_group_id):
        """
        sets the vpc security group for a particular cluster, using an
        exponential backoff to check status.

        :param cluster_name: name of cluster
        :type cluster_name: string
        :param vpc_security_group_id: id of vpc security group
        :type vpc_security_group_id: string
        """
        if not vpc_security_group_id:
            return

        response = self._conn.modify_cluster(
            cluster_name, vpc_security_group_ids=[vpc_security_group_id],
        )
        cluster_status = get_deep(
            response,
            'ModifyClusterResponse.ModifyClusterResult.Cluster'
        )

        sec_groups = cluster_status['VpcSecurityGroups']

        while True:
            if len(sec_groups) != 1:
                raise ValueError("Unexpected number of security groups")
            sg_id = sec_groups[0]['VpcSecurityGroupId']
            sg_status = sec_groups[0]['Status']

            if sg_id != vpc_security_group_id:
                raise ValueError(
                    "Unexpected vpc security group id: {0}".format(sg_id)
                )
            if sg_status == 'adding':
                time.sleep(self.poll_interval_sec)
                response = self.get_cluster_info(cluster_name)
                sec_groups = get_deep(
                    response,
                    'VpcSecurityGroups'
                )
                sys.stderr.write("vpc security group status: adding\n")
            elif sg_status == 'active':
                sys.stderr.write("vpc security group status: active\n")
                break
            else:
                raise ValueError(
                    "Unexpected vpc security group status: {0}".format(sg_status)
                )

        sys.stderr.write("set security group: {0}\n".format(vpc_security_group_id))

    def set_security_group(self, cluster_name, security_group):
        """
        sets the security group for a particular cluster

        :param cluster_name: name of cluster
        :type cluster_name: string
        :param security_group: name of a security group
        :type security_group: string
        """
        if not security_group:
            return

        response = self._conn.modify_cluster(
            cluster_name, cluster_security_groups=[security_group],
        )
        cluster_status = get_deep(
            response,
            'ModifyClusterResponse.ModifyClusterResult.Cluster'
        )

        sec_groups = cluster_status['ClusterSecurityGroups']
        if len(sec_groups) != 1:
            raise ValueError("Unexpected number of security groups")
        sg_name = sec_groups[0]['ClusterSecurityGroupName']
        sg_status = sec_groups[0]['Status']

        if sg_name != security_group:
            raise ValueError(
                "Unexpected security group name: {0}".format(sg_name)
            )
        if sg_status != 'active':
            raise ValueError(
                "Unexpected security group status: {0}".format(sg_status)
            )
        sys.stderr.write("set security group: {0}\n".format(security_group))

    def set_parameter_group(self, cluster_name, param_group):
        """
        sets the parameter group for a particular cluster, using an
        exponential backoff to check status.

        :param cluster_name: name of cluster
        :type cluster_name: string
        :param param_group: name of a parameter group
        :type param_group: string

        :returns: a pair of cluster status and parameter group apply status
        :rtype: string
        """
        if not param_group:
            return

        response = self._conn.modify_cluster(
            cluster_name, cluster_parameter_group_name=param_group,
        )
        cluster = get_deep(
            response,
            'ModifyClusterResponse.ModifyClusterResult.Cluster'
        )

        pg_groups = cluster['ClusterParameterGroups']
        if len(pg_groups) != 1:
            raise ValueError("Unexpected number of parameter groups")
        pg_name = pg_groups[0]['ParameterGroupName']
        pg_status = pg_groups[0]['ParameterApplyStatus']
        if pg_name != param_group:
            raise ValueError(
                "Unexpected parameter group name: {0}".format(pg_name)
            )
        if pg_status != 'applying':
            raise ValueError(
                "Unexpected parameter group status: {0}".format(pg_status)
            )
        sys.stderr.write("set parameter group: {0}\n".format(param_group))

        cluster_status = "{0},{1}".format(cluster['ClusterStatus'], pg_status)
        while True:
            sys.stderr.write("cluster_status: {0}\n".format(cluster_status))
            if cluster_status == 'available,pending-reboot':
                time.sleep(self.poll_interval_sec)
                response = self._conn.reboot_cluster(cluster_name)
                cluster = get_deep(
                    response,
                    'RebootClusterResponse.RebootClusterResult.Cluster'
                )
                cluster_status = "{0},{1}".format(
                    cluster['ClusterStatus'],
                    cluster[
                        'ClusterParameterGroups'
                    ][0]['ParameterApplyStatus']
                )
                if cluster_status != 'rebooting,pending-reboot':
                    raise ValueError(
                        'Unexpected cluster status: {0}'.format(cluster_status)
                    )
                break
            if cluster_status == 'available,in-sync':
                break
            time.sleep(self.poll_interval_sec)
            cluster_status = self.get_cluster_and_pg_status(cluster_name)

        while True:
            sys.stderr.write("cluster_status: {0}\n".format(cluster_status))
            if cluster_status == 'available,in-sync':
                break
            time.sleep(self.poll_interval_sec)
            cluster_status = self.get_cluster_and_pg_status(cluster_name)

    def restore_from_cluster_snapshot(self, cluster_name, snapshot_name,
                                      param_group=None, vpc_group_id=None,
                                      cluster_subnet_group_name=None,
                                      wait_for_cluster_availability=True):
        """Restore cluster from snapshot

        Wait until cluster and restore status indicate success

        :param cluster_name: the name of the cluster we restore to
        :type cluster_name: string
        :param snapshot_name: the name of the snapshot from which we restore
        :type snapshot_name: string
        :param param_group: parameter group of the redshift cluster
        :type param_group: string
        :param vpc_group_id: vpc group id of the redshift cluster
        :type vpc_group_id: string
        :param cluster_subnet_group_name: the name of the subnet group where
                                          we want the cluster restored
        :type cluster_subnet_group_name: string
        :param wait_for_cluster_availability: determine whether wait until the
                                              the cluster is ready or not
        :type wait_for_cluster_availability: boolean
        """
        snapshot_response = self._conn.describe_cluster_snapshots(
            snapshot_identifier=snapshot_name
        )

        snapshot_list = get_deep(
            snapshot_response, 'DescribeClusterSnapshotsResponse.\
DescribeClusterSnapshotsResult.Snapshots'
        )
        if len(snapshot_list) > 1:
            raise ValueError("{0}: too many snapshots".format(snapshot_name))

        snapshot = snapshot_list.pop()
        if snapshot['EstimatedSecondsToCompletion'] != 0:
            raise ValueError('snapshot not ready')
        if snapshot['Status'] != 'available':
            raise ValueError("snapshot status: {0}".format(snapshot['Status']))
        restore_response = self._conn.restore_from_cluster_snapshot(
            cluster_name,
            snapshot_name,
            cluster_subnet_group_name=cluster_subnet_group_name,
            cluster_parameter_group_name=param_group,
            vpc_security_group_ids=[vpc_group_id]
        )

        cluster_status = get_deep(
            restore_response, 'RestoreFromClusterSnapshotResponse.\
RestoreFromClusterSnapshotResult.Cluster.ClusterStatus'
        )

        while wait_for_cluster_availability:
            sys.stderr.write("cluster_status: {0}\n".format(cluster_status))
            if cluster_status == 'available,completed':
                break
            time.sleep(self.poll_interval_sec)
            cluster_status = self.get_cluster_and_restore_status(cluster_name)

    def delete_cluster(self, cluster_name, save_snapshot=True):
        """
        Delete cluster, optionally save snapshot

        :param cluster_name: name of the cluster to be deleted
        :type cluster_name: string
        :param save_snapshot: True if we should save a snapshot, False if not
        :type save_snapshot: boolean
        """
        snapshot_name = "{0}-{1}".format(
            cluster_name,
            datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S")
        )

        try:
            if save_snapshot:
                sys.stderr.write(
                    "\ncreating final snapshot: {0}\n".format(snapshot_name)
                )
                delete_response = self._conn.delete_cluster(
                    cluster_name,
                    skip_final_cluster_snapshot=False,
                    final_cluster_snapshot_identifier=snapshot_name
                )
            else:
                delete_response = self._conn.delete_cluster(
                    cluster_name,
                    skip_final_cluster_snapshot=True,
                )
        except ClusterNotFound:
            # no cluster, nothing to delete, no snapshot to save
            return

        cluster_status = get_deep(
            delete_response,
            'DeleteClusterResponse.DeleteClusterResult.Cluster.ClusterStatus',
        )

        # poll until triggering ClusterNotFound exception
        while cluster_status is not None:
            sys.stderr.write("cluster_status: {0}\n".format(cluster_status))
            time.sleep(self.poll_interval_sec)
            try:
                cluster_status = self.get_cluster_status(cluster_name)
            except ClusterNotFound as e:
                sys.stderr.write(str(e))
                cluster_status = None
