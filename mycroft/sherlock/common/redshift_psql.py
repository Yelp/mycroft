# -*- coding: utf-8 -*-
"""
redshift_psql.py is a collection of functions used to load data from s3 into
redshift

Example Instantiation:

    rspg = RedshiftPostrges("config.yaml", "stream_name", "pg_auth_file",
        run_local=True)

"""

import socket
import time
from datetime import datetime

import boto
import staticconf
from dateutil.parser import parse as parsedate
from staticconf import read_string
from staticconf import YamlConfiguration

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extensions import QueryCanceledError
from sherlock.common.aws import get_aws_creds

ADD_SCHEMA_PATH = "SET search_path TO '$user', public, %(schema_path)s"
DEFAULT_NAMESPACE = "public"

# Copied from http://initd.org/psycopg/articles/2014/07/20/cancelling-postgresql-statements-python/
from select import select
from psycopg2.extensions import POLL_OK, POLL_READ, POLL_WRITE


def wait_select_inter(conn):
    while True:
        try:
            state = conn.poll()
            if state == POLL_OK:
                break
            elif state == POLL_READ:
                select([conn.fileno()], [], [])
            elif state == POLL_WRITE:
                select([], [conn.fileno()], [])
            else:
                raise conn.OperationalError(
                    "bad state from poll: %s" % state)
        except KeyboardInterrupt:
            conn.cancel()
            # the loop will be broken by a server error
            continue


def get_namespaced_tablename(tablename, schemaname=None):
    if schemaname is None:
        rs_schema = get_redshift_schema()
    else:
        # note we do lower for backward compatability
        rs_schema = schemaname.lower()
    if rs_schema == DEFAULT_NAMESPACE:
        return tablename
    return rs_schema + "." + tablename


def get_redshift_schema():
    # note we do lower for backward compatability
    return read_string('redshift_schema', DEFAULT_NAMESPACE).lower()


class RedshiftPostgres(object):
    """
    This class simplifies running queries on redshift.  The current purpose is
    for creating tables, and copying data into them from S3.  However, it can
    be used for general SQL commands.

    Constructor Args:
    logdir -- the directory where the logs go
    logstrm -- a PipelineStreamLogger to record starts, completes and
               failed sql commands
    psql_auth_file -- the file from which we get a username and password for a
        redshift account
    run_local -- whether to run locally or not
    """

    # this should give 1 hour for a sql command to complete
    SECONDS_BEFORE_SENDING_PROBE = 1
    SECONDS_BETWEEN_SENDING_PROBE = 60
    RETRIES_BEFORE_QUIT = 60

    def __init__(self, logstrm, psql_auth_file, run_local=False):

        self.run_local = run_local
        self.host = staticconf.read_string('redshift_host')
        self.port = staticconf.read_int('redshift_port')
        private_dict = YamlConfiguration(psql_auth_file)
        self.user = private_dict['redshift_user']
        self.password = private_dict['redshift_password']
        self.log_stream = logstrm
        self._aws_key = ''
        self._aws_secret = ''
        self._aws_token = ''
        self._aws_token_expiry = datetime.utcnow()
        self._whitelist = ['select', 'create', 'insert', 'update']
        self._set_aws_auth()
        psycopg2.extensions.set_wait_callback(wait_select_inter)

    def _set_aws_auth(self):
        """
        _set_aws_auth gets key, secret, token and expiration either from a
        file or from a temporary instance and sets them
        """

        cred_tuple = get_aws_creds(self.run_local)
        self._aws_key = cred_tuple.access_key_id
        self._aws_secret = cred_tuple.secret_access_key
        self._aws_token = cred_tuple.token
        self._aws_token_expiry = parsedate(cred_tuple.expiration)

    def get_boto_config(self):
        boto_dict = {}
        for section in boto.config.sections():
            boto_dict[section] = {}
            for option in boto.config.options(section):
                if option != 'aws_secret_access_key':
                    boto_dict[section][option] = boto.config.get(section, option)
                else:
                    boto_dict[section][option] = "xxxxxxxxxxxxxxxx"
        return boto_dict

    def get_connection(self, database):
        """
        gets a connection to the a psql database

        Args:
            self.password -- the password to the database

        Returns:
            a connection object
        """
        # additional logging to help with connection issues
        boto_config_dict = self.get_boto_config()
        self.log_stream.write_msg('boto', extra_msg=boto_config_dict)
        log_template = "getting connection with host {0} port {1} user {2} db {3}"
        log_msg = log_template.format(self.host, self.port, self.user, database)
        self.log_stream.write_msg('starting', extra_msg=log_msg)

        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=database,
            sslmode='require')

        self.log_stream.write_msg('finished', extra_msg=log_msg)

        fd = conn.fileno()
        sock = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE,
                        self.SECONDS_BEFORE_SENDING_PROBE)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL,
                        self.SECONDS_BETWEEN_SENDING_PROBE)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT,
                        self.RETRIES_BEFORE_QUIT)

        return conn

    def cleanse_sql(self, command):
        """
        cleanses a psql command of any auth information

        Args:
            command -- the psql command

        Returns:
            the cleansed command
        """
        cmd_list = command.split()
        first_word = cmd_list[0]
        if first_word.lower() in self._whitelist:
            return " ".join(cmd_list)
        return cmd_list[0] + " cleansed "

    def run_sql_ex(self, sql, database, log_msg, s3_needed=False, params=None,
                   output=False, time_est_secs=10, need_commit=True,
                   schema=DEFAULT_NAMESPACE):
        """
        run_sql takes a command and executes using the connection found
        in get_conection.

        Args:
            sql -- the postgres command to run
            database -- the database on which the command is to be run
            log_msg -- a shortened message for what command we're running
            s3_needed  -- if the sql command requires s3 input this = True,
                otherwise it's false.  For example, a simple query of a table
                would have s3_needed=False, while COPY from S3 would have
                s3_needed=True.
            params -- if there are any parameters for the command
            output -- if the command Returns rows (e.g., a SELECT command))
            time_est_secs -- how long the user estimates the command to run
                in seconds.  This is used to decide whether to get new certs
                or not
            need_commit -- False if command does not need
                to be committed (ex: vacuum)
            schema -- the schema in the database on which the command is run
                      anything other than the default namespace must have the
                      schemaname added in the search_path. This ephemeral so
                      must be done on a per-session basis, and since we close
                      the cursor and connecion after each query we'll check
                      every time.
        Returns:
            if there's a return value, it is the results of the query
        """

        start_time = time.time()
        self.log_stream.write_msg('starting', extra_msg=log_msg)
        exception = None

        if s3_needed:
            try:
                if self._aws_token is None:
                    sql = sql.replace(';token=%s', '')
                    sql = sql % (self._aws_key, self._aws_secret)
                else:
                    sql = sql % (self._aws_key, self._aws_secret, self._aws_token)
            except TypeError as type_error:
                self.log_stream.write_msg(
                    'error', error_msg=repr(type_error), extra_msg=sql
                )
                raise

        try:
            result = dict()
            with self.get_connection(database) as conn:
                if need_commit is False:
                    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor()
                if schema != DEFAULT_NAMESPACE:
                    schema_params = {'schema_path': schema}
                    cur.execute(ADD_SCHEMA_PATH, schema_params)
                if params:
                    cur.execute(sql, params)
                else:
                    cur.execute(sql)
                if output:
                    rows = cur.fetchall()
                result['status'] = cur.statusmessage
                cur.close()

            self.log_stream.write_msg(
                'finished', job_start_secs=start_time, extra_msg=log_msg
            )
            if output:
                result['output'] = rows
            return result
        except QueryCanceledError as cmd_exception:
            exception = cmd_exception
            raise KeyboardInterrupt
        except Exception as cmd_exception:
            exception = cmd_exception
            raise
        finally:
            self.log_stream.write_msg(
                'error',
                job_start_secs=start_time,
                error_msg=repr(exception), extra_msg=log_msg
            )

    def run_sql(self, sql, database, log_msg, s3_needed=False, params=None,
                output=False, time_est_secs=10, need_commit=True,
                schema=DEFAULT_NAMESPACE):
        result = self.run_sql_ex(
            sql, database, log_msg, s3_needed, params,
            output, time_est_secs, need_commit, schema=schema)
        if result is False:
            return False
        return result['output'] if output is True else True
