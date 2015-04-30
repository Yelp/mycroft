# -*- coding: utf-8 -*-
import staticconf
from mycroft.log_util import log_exception
from mycroft.models import aws_connections
from boto.sqs.jsonmessage import JSONMessage

from boto.exception import BotoClientError
from boto.exception import BotoServerError


class SQSWrapper(object):

    """ Wrapper around boto's sqs interface.
    Used by Mycroft's backend components: worker and scanner, when interacting
    with SQS.
    """

    def __init__(self, queue_name, msg_class=JSONMessage):
        self._num_messages_to_fetch = staticconf.get_int(
            'sqs.num_messages_per_fetch', 1
        )
        self._wait_time_secs = staticconf.get_int(
            'sqs.wait_time_secs', 10
        )
        self._queue = self._get_queue(queue_name)
        if self._queue is None:
            raise ValueError(
                "Queue not found, for queue name:" + queue_name)
        self.msg_class = msg_class
        self._queue.set_message_class(self.msg_class)
        self.queue_name = queue_name
        self.attributes = self._queue.get_attributes()

    def get_messages_from_queue(self):
        """ Fetches messages from the sqs queue of name passed in during this
        object's construction.
        Does not handle exceptions from Boto.

        :rtype: a list of SQS message or None
        """
        try:
            msgs = self._queue.get_messages(
                num_messages=self._num_messages_to_fetch,
                wait_time_seconds=self._wait_time_secs)
            return msgs
        except (BotoClientError, BotoServerError):
            log_exception(
                "Boto exception in fetching messages from SQS, for queue name:"
                + self._queue.id)
            raise

    def write_message_to_queue(self, msg):
        """ Write arbitrary data to sqs queue

        :param msg: data to write
        """
        message = self.msg_class()
        message.set_body(msg)
        self._queue.write(message)

    def delete_message_from_queue(self, msg):
        """ Delete given message from sqs. Throws exceptions from sqs to
        the caller.

        :param msg: a message obtained from sqs
        """
        self._queue.delete_message(msg)

    def delete_message_batch_from_queue(self, msgs):
        self._queue.delete_message_batch(msgs)

    def clear(self):
        """Delete all messages from sqs
        """
        self._queue.clear()

    def _get_queue(self, queue_name):
        """ Get the SQS queue with the given name from boto.sqs.

        :param queue_name: name of the SQS queue, string
        :rtype: boto sqs queue object
        """
        conn = aws_connections.get_sqs_connection()
        return conn.get_queue(queue_name)

    def get_queue_attributes(self):
        """ Get sqs queue attributes
        """
        return self.attributes

    def get_queue_name(self):
        """ Get queue name of the sqs
        """
        return self.queue_name

    def get_wait_time(self):
        return self._wait_time_secs
