import argparse
import json
import os
import subprocess
import time
import yaml
from collections import namedtuple

from boto import cloudformation
from boto import ec2
from boto import redshift
from boto.exception import BotoServerError


RedshiftCluster = namedtuple('RedshiftCluster', ['port', 'host'])


class CloudFormationStack(object):
    MYCROFT_CONFIG_DIR = 'mycroft_config'

    AWS_REQUIRED_FIELDS = ['aws_secret_access_key', 'aws_access_key_id']
    REQUIRED_CREATION_ARGS = ['redshift_master_user', 'redshift_master_user_password', 'vpc_id']

    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self._parse_input_args()
        self._validate_input_args()
        self._verify_docker_command()

        self._aws_access_key_id, self._aws_secret_access_key = self._get_aws_creds(self.input_args)
        self._region_name = self.input_args.region_name
        self._stack_name = "Mycroft"
        self._master_user = self.input_args.redshift_master_user
        self._master_user_password = self.input_args.redshift_master_user_password
        self._key_pair_name = self.input_args.emr_key_pair_name
        self.config_to_resource_map = {}
        self.config_to_resource_map['sqs'] = {}
        self.config_to_resource_map['sqs']['region'] = self._region_name
        self.config_to_resource_map['aws_config'] = {}
        self.config_to_resource_map['aws_config']['region'] = self._region_name

    def _parse_input_args(self):
        self.parser.add_argument(
            "--aws-creds-file",
            help="path to a file containing your aws key and secret",
            required=True
        )

        self.parser.add_argument(
            "--redshift-master-user",
            help="the master user name of the redshift cluster to be created",
        )
        self.parser.add_argument(
            "--redshift-master-user-password",
            help="the master user password of the redshift cluster to be created",
        )
        self.parser.add_argument(
            "--vpc-id",
            help="the id of the vpc the cloud formation should be created in",
            type=str,
        )
        self.parser.add_argument(
            "--region-name",
            help="the region in which we create the cloud formation stack",
            type=str,
            default="us-west-2"
        )
        self.parser.add_argument(
            "--smtp-login",
            help="A login to your smtp server from which mycroft will send job status emails",
            type=str,
            required=True
        )
        self.parser.add_argument(
            "--smtp-password",
            help="A password for the username from which mycroft will send job status emails",
            type=str,
            required=True
        )
        self.parser.add_argument(
            "--smtp-host",
            help="The host from which mycroft will send job status emails",
            type=str,
            default="smtp.gmail.com"
        )
        self.parser.add_argument(
            "--smtp-port",
            help="The e-mail port on your smtp host",
            type=str,
            default="587"
        )
        self.parser.add_argument(
            "--smtp-security",
            help="Security used for e-mail: either TLS or SSL",
            type=str,
            choices=['TLS', 'SSL'],
            default="TLS"
        )

        self.parser.add_argument(
            "--emr-key-pair-name",
            help="Name of EC2 key pair to create to allow easier debugging of MRJobs",
            type=str,
            default="MycroftEMRKeyPair"
        )

        cf_stack_group = self.parser.add_mutually_exclusive_group()

        cf_stack_group.add_argument(
            "--delete-cf-stack",
            help="deletes the cloud formation stack on aws",
            action='store_true'
        )

        cf_stack_group.add_argument(
            "--skip-cf-stack-creation",
            help="this will skip the creation of the cf stack, useful if you want to reconfigure other options",
            action='store_true'
        )

        self.parser.add_argument(
            "--skip-docker-image-build",
            help="skips building the docker image",
            action='store_true'
        )
        self.input_args = self.parser.parse_args()

    def _validate_input_args(self):
        if not self.input_args.delete_cf_stack and self._is_missing_required_creation_args():
            self.parser.error(
                "Missing required creation arguments.  Required Arguments: %s" % ", ".join(self.REQUIRED_CREATION_ARGS)
            )

    def _is_missing_required_creation_args(self):
        return any(getattr(self.input_args, arg) is None for arg in self.REQUIRED_CREATION_ARGS)

    def _creds_to_dict(self, f):
        try:
            y = yaml.load(f)
        finally:
            if not all(field in y for field in self.AWS_REQUIRED_FIELDS):
                self._abort(
                    "aws_secret_access_key and aws_access_key_id are both "
                    "required in the credentials file.  See "
                    "mycroft_config/aws_creds.example.yaml for an example."
                )
            return y

    def _get_aws_creds(self, input_args):
        with open(input_args.aws_creds_file, 'r') as f:
            y = self._creds_to_dict(f)
        return y['aws_access_key_id'], y['aws_secret_access_key']

    def _get_cf_template_json(self):
        with open('mycroft_config/mycroft_with_cluster.template', 'r') as f:
            j = json.load(f)
            starter_cluster_properties = j["Resources"]["MycroftRedshiftCluster"]["Properties"]
            starter_cluster_properties["MasterUsername"] = self._master_user
            starter_cluster_properties["MasterUserPassword"] = self._master_user_password
            return json.dumps(j)

    def _get_cf_connection(self):
        return self._get_connection(cloudformation)

    def _get_ec2_connection(self):
        return self._get_connection(ec2)

    def _get_connection(self, service):
        return service.connect_to_region(
            self._region_name,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key
        )

    def run(self):
        if self.input_args.delete_cf_stack:
            self.delete_cf_stack()
        else:
            if not self.input_args.skip_cf_stack_creation:
                self.create_cf_stack()
                self.create_emr_key_pair()

            self._display_status("Configuring mycroft with cloud formation information")
            self.write_config_yaml()
            self.write_private()
            self.write_mrjob_yaml()
            self.write_session_file()

            if not self.input_args.skip_docker_image_build:
                self._build_docker_image()
                self._add_cluster_to_mycroft()

            self.display_setup_information()

    def create_cf_stack(self):
        self._display_status("Creating cloud formation stack.  This could take a few minutes...")
        cf_template = self._get_cf_template_json()
        conn = self._get_cf_connection()
        if self._does_stack_exist(conn):
            self.parser.error(
                "CloudFormation Stack Already Exists!  Either skip creating "
                "it with --skip-cf-stack-creation or delete it and retry "
                "by using --delete-cf-stack."
            )
        conn.create_stack(
            self._stack_name,
            template_body=cf_template,
            parameters=[('VpcId', self.input_args.vpc_id)],
            capabilities=['CAPABILITY_IAM']
        )
        self._poll_stack_status()

    def create_emr_key_pair(self):
        if not os.path.exists(self._get_key_pair_file_path()):
            conn = self._get_ec2_connection()
            key_pair = conn.create_key_pair(self._key_pair_name)
            key_pair.save("mycroft_config")

    def describe_cf_stack_resources(self):
        conn = self._get_cf_connection()
        description = conn.describe_stack_resources(stack_name_or_id=self._stack_name)
        return description

    def describe_cf_stack_resource(self, resource_id):
        conn = self._get_cf_connection()
        description = conn.describe_stack_resource(stack_name_or_id=self._stack_name, logical_resource_id=resource_id)
        return description

    def delete_cf_stack(self):
        self._display_status("Deleting Cloud Formation Stack.  This could take a few minutes...")
        conn = self._get_cf_connection()
        conn.delete_stack(stack_name_or_id=self._stack_name)
        delete_status = self._wait_for_stack_status_change("DELETE_IN_PROGRESS")
        if delete_status != "DELETE_COMPLETE":
            bucket = self.l_to_p_map.get('MycroftS3Bucket')
            bucket_message = "" if bucket is None else (
                "Note that if Mycroft has been used, cloud formation cannot remove the s3 bucket\n"
                "unless it is emptied first.  Use the aws command to recursively remove the\n"
                "contents of the bucket, then try rerunning this deletion command:\n\n"
                "aws s3 rm s3://{bucket}/ --recursive"
            ).format(bucket=bucket)

            self._abort(
                """
Cloud Formation stack deletion failed -- please see your AWS console for details

{bucket_message}
                """.format(bucket_message=bucket_message)
            )
        else:
            self._display_status("Cloud formation delete complete")

    def _does_stack_exist(self, conn):
        try:
            if len(conn.describe_stacks(stack_name_or_id=self._stack_name)) > 0:
                return True
        except BotoServerError:
            # Describing will raise a server error if the stack doesn't exist
            pass
        return False

    def _poll_stack_status(self):
        create_status = self._wait_for_stack_status_change("CREATE_IN_PROGRESS")
        if create_status != "CREATE_COMPLETE":
            self._abort("stack creation failed -- please see your AWS console for details")

    def _wait_for_stack_status_change(self, waiting_for_change_from_status):
        conn = self._get_cf_connection()
        stack = conn.describe_stacks(stack_name_or_id=self._stack_name)[0]
        stack_id = stack.stack_id
        new_status = stack.stack_status
        while new_status == waiting_for_change_from_status:
            self._display_status("stack status: %s" % new_status)
            time.sleep(10)
            # Using the stack id here since that works for clusters that are
            # deleted
            new_status = conn.describe_stacks(stack_name_or_id=stack_id)[0].stack_status
        return new_status

    def _get_logical_to_physical_resource_map(self):
        resources = self.describe_cf_stack_resources()
        logical_to_physical_map = {}
        for r in resources:
            phys_id = r.physical_resource_id
            logical_to_physical_map[r.logical_resource_id] = phys_id.split('/')[-1] if "http" in phys_id else phys_id
        return logical_to_physical_map

    def update_config_parameters(self, yaml_config):
        yaml_config['sqs']['et_queue_name'] = self.l_to_p_map["ETQueueV1"]
        yaml_config['sqs']['et_scanner_queue_name'] = self.l_to_p_map["ETScannerQueueV1"]
        yaml_config['aws_config']['scheduled_jobs_table'] = self.l_to_p_map["ScheduledJobs"]
        yaml_config['aws_config']['etl_records_table'] = self.l_to_p_map["ETLRecords"]
        yaml_config['aws_config']['redshift_clusters'] = self.l_to_p_map["RedshiftClusters"]
        yaml_config['run_local']['private'] = "/mycroft_config/private.yaml"
        yaml_config['run_local']['session_file'] = "/mycroft_config/session_file.txt"
        yaml_config['run_local']['mrjob_arg_template'] = "--runner=emr {0} --output-dir={1} --no-output --extractions {3} --column-delimiter={4} -c /mycroft_config/mrjob.conf"

        yaml_config['s3_bucket'] = self.l_to_p_map['MycroftS3Bucket']
        yaml_config['s3_schema_location_format'] = 's3://{bucket}/mycroft/stream/{{0}}/schema/{{1}}/schema.yaml'.format(bucket=self.l_to_p_map['MycroftS3Bucket'])
        yaml_config['s3_mrjob_output_path_format'] = 's3://{bucket}/mycroft/scratch/{{0}}/{{1}}/{{2}}'.format(bucket=self.l_to_p_map['MycroftS3Bucket'])
        yaml_config['smtp_host'] = self.input_args.smtp_host
        yaml_config['smtp_port'] = self.input_args.smtp_port
        yaml_config['smtp_login'] = self.input_args.smtp_login
        yaml_config['smtp_password'] = self.input_args.smtp_password
        yaml_config['smtp_security'] = self.input_args.smtp_security

        return yaml_config

    def update_mrjob_parameters(self, yaml_mrjob):
        yaml_mrjob['runners']['emr']['aws_access_key_id'] = self._aws_access_key_id
        yaml_mrjob['runners']['emr']['aws_secret_access_key'] = self._aws_secret_access_key

        yaml_mrjob['runners']['emr']['s3_log_uri'] = "s3://%s/emr/logs/" % self.l_to_p_map['MycroftS3Bucket']
        yaml_mrjob['runners']['emr']['s3_scratch_uri'] = "s3://%s/emr/scratch/" % self.l_to_p_map['MycroftS3Bucket']

        yaml_mrjob['runners']['emr']['ec2_key_pair'] = self._key_pair_name
        yaml_mrjob['runners']['emr']['ec2_key_pair_file'] = "/{0}".format(self._get_key_pair_file_path())
        yaml_mrjob['runners']['emr']['emr_api_params']['ServiceRole'] = self.l_to_p_map["MycroftEMRRole"]
        yaml_mrjob['runners']['emr']['iam_job_flow_role'] = self.l_to_p_map["MycroftEMREC2InstanceProfile"]

        return yaml_mrjob

    def write_config_yaml(self):
        with open('mycroft_config/config-env-docker.template.yaml', 'r') as f:
            y = yaml.load(f)
            y = self.update_config_parameters(y)

        with open('mycroft_config/config-env-docker.yaml', 'w') as f:
            f.write(
                yaml.safe_dump(y, default_flow_style=False)
            )

    def write_private(self):
        with open('mycroft_config/private.yaml', 'w') as f:
            private_dict = {}
            private_dict['redshift_user'] = self.input_args.redshift_master_user
            private_dict['redshift_password'] = self.input_args.redshift_master_user_password
            f.write(yaml.safe_dump(private_dict, default_flow_style=False))

    def write_mrjob_yaml(self):
        with open('mycroft_config/mrjob.template.conf', 'r') as f:
            y = yaml.load(f)
            y = self.update_mrjob_parameters(y)

        with open('mycroft_config/mrjob.conf', 'w') as f:
            f.write(
                yaml.safe_dump(y, default_flow_style=False)
            )

    def write_session_file(self):
        with open('mycroft_config/session_file.txt', 'w') as f:
            d = {
                "AccessKeyId": self._aws_access_key_id,
                "SecretAccessKey": self._aws_secret_access_key
            }
            f.write(json.dumps(d))

    def display_setup_information(self):
        self._display_status(
            (
                "\n\n\n{build_message}\n\n"
                "Redshift Cluster Information:\n"
                "    host: \"{redshift_cluster.host}\"\n"
                "    port: \"{redshift_cluster.port}\"\n\n"
                "To remove the cloud formation resources that Mycroft has\n"
                "provisioned on AWS, rerun this command with the\n"
                "--delete-cf-stack option.\n\n"
                "To start using Mycroft now, just run `docker-compose up` and\n"
                "point your browser at port 11476 of your docker host."
            ).format(
                build_message=self._get_build_status_message(),
                redshift_cluster=self.redshift_cluster
            )
        )

    def _get_build_status_message(self):
        if self.input_args.skip_docker_image_build:
            return (
                "Mycroft has been successfully configured.  To build the image run:\n"
                "   docker build -t mycroft ."
            )
        else:
            return "Mycroft has been successfully built and configured."

    def _get_key_pair_file_path(self):
        return os.path.join(
            self.MYCROFT_CONFIG_DIR,
            "{0}.pem".format(self._key_pair_name)
        )

    def _verify_docker_command(self):
        try:
            with open(os.devnull, 'wb') as devnull:
                subprocess.check_call('docker ps', shell=True, stdout=devnull, stdin=devnull, stderr=devnull)
        except subprocess.CalledProcessError:
            self._abort("docker command could not run, verify docker ps runs without error")

    def _build_docker_image(self):
        self._display_status("Building docker image, this could take a few minutes...")
        try:
            directory = os.path.dirname(os.path.realpath(__file__))
            subprocess.check_call('docker build -t mycroft {0}'.format(directory), shell=True)
        except:
            self._abort("docker build failed.  See the logs above.")
        self._display_status("Building docker image completed")

    def _add_cluster_to_mycroft(self):
        self._display_status("Adding cluster to mycroft...")
        subprocess.check_call(
            (
                "docker run mycroft python -m mycroft.batch.add_cluster "
                "--cluster-name=\"Mycroft\" "
                "--redshift-host=\"{redshift_cluster.host}\" "
                "--redshift-port={redshift_cluster.port} "
                "--idempotent"
            ).format(redshift_cluster=self.redshift_cluster),
            shell=True
        )
        self._display_status("Cluster added to mycroft")

    @property
    def redshift_cluster(self):
        if not hasattr(self, '_redshift_cluster'):
            conn = self._get_connection(redshift)
            cluster = self.l_to_p_map["MycroftRedshiftCluster"]
            response = conn.describe_clusters(cluster)
            endpoint_info = response['DescribeClustersResponse']['DescribeClustersResult']['Clusters'][0]['Endpoint']
            self._redshift_cluster = RedshiftCluster(
                host=endpoint_info['Address'],
                port=endpoint_info['Port']
            )
        return self._redshift_cluster

    @property
    def l_to_p_map(self):
        if not hasattr(self, '_l_to_p_map'):
            self._l_to_p_map = self._get_logical_to_physical_resource_map()
        return self._l_to_p_map

    def _abort(self, message):
        print message
        exit(1)

    def _display_status(self, message):
        print message


if __name__ == '__main__':
    cloudFormationStack = CloudFormationStack()
    cloudFormationStack.run()
