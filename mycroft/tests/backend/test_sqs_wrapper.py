# -*- coding: utf-8 -*-
import mock
import pytest
import staticconf.testing

from boto.exception import BotoServerError

from mycroft.backend.sqs_wrapper import SQSWrapper
from mycroft.models.aws_connections import get_boto_creds

from tests.data.mock_config import MOCK_CONFIG

""" Create a few fake objects (fake connection to SQS, fake SQS queue) for use
in tests below. Rely on duck typing for the code under test to call these
methods.
"""

# TODO: Update this test to use an inmemory SQS rather than rely on duck typing
# Do away with all Fake* classes below.


class FakeConn(object):

    queue = None

    def set_queue(self, queue):
        self.queue = queue

    def get_queue(self, queue_name):
        return self.queue


class FakeQueue(object):

    id = "a"
    msgs = None
    exception = False

    def set_messages(self, msgs):
        self.msgs = msgs

    def get_messages(self, num_messages, wait_time_seconds):
        if self.exception:
            raise BotoServerError(503, "test")
        return [] if self.msgs is None else self.msgs

    def set_message_class(self, class_type):
        self.class_type = class_type

    def delete_message(self, msg):
        self.deleted_msg = msg

    def delete_message_batch(self, msgs):
        self.deleted_msgs = msgs

    def clear(self):
        self.msgs = None

    def get_attributes(self):
        return {'MessageRetentionPeriod': 4 * 24 * 3600}


@pytest.yield_fixture
def get_mock_boto():
    with staticconf.testing.MockConfiguration(MOCK_CONFIG):
        with mock.patch('boto.sqs.connect_to_region') as mock_boto:
            yield mock_boto


def get_test_creds():
    with staticconf.testing.MockConfiguration(MOCK_CONFIG):
        return get_boto_creds()


def test_constructor_throws_on_queue_being_none(get_mock_boto):
    mock_obj = get_mock_boto
    fake_conn = FakeConn()

    mock_obj.return_value = fake_conn
    with pytest.raises(ValueError):
        SQSWrapper("test")
    mock_obj.assert_called_once_with('us-west-2', **get_test_creds())


def test_set_class_type_works_fine(get_mock_boto):
    mock_obj = get_mock_boto
    fake_conn = FakeConn()
    fake_conn.queue = FakeQueue()
    mock_obj.return_value = fake_conn
    SQSWrapper("test", object)
    mock_obj.assert_called_once_with('us-west-2', **get_test_creds())

    assert fake_conn.queue.class_type == object


def test_init_variables_initialized_from_config_correctly(get_mock_boto):
    mock_obj = get_mock_boto
    fake_conn = FakeConn()
    fake_queue = FakeQueue()
    fake_conn.set_queue(fake_queue)

    mock_obj.return_value = fake_conn
    sqs = SQSWrapper("testq", None)
    assert sqs._num_messages_to_fetch == 2, "expected num_msgs_per_fetch to \
        be 2 from config"
    assert sqs._wait_time_secs == 20, "expected wait_time_secs to be 20 from \
        config"
    assert sqs._queue is not None, "queue should not be None at \
        end of constructor"
    mock_obj.assert_called_once_with('us-west-2', **get_test_creds())


def test_get_messages_successful_case_no_errors(get_mock_boto):
    fake_conn = FakeConn()
    fake_queue = FakeQueue()
    fake_conn.set_queue(fake_queue)
    fake_queue.set_messages([object()])

    mock_obj = get_mock_boto
    mock_obj.return_value = fake_conn
    sqs = SQSWrapper("some-queue", None)
    ret = sqs.get_messages_from_queue()
    assert len(ret) == 1, "Expected 1 message from sqs queue"
    mock_obj.assert_called_once_with('us-west-2', **get_test_creds())


def test_get_messages_exception_thrown_out(get_mock_boto):
    fake_conn = FakeConn()
    fake_queue = FakeQueue()
    fake_conn.set_queue(fake_queue)
    fake_queue.exception = True  # set it to throw boto error
    mock_obj = get_mock_boto
    mock_obj.return_value = fake_conn
    sqs = SQSWrapper("some-queue", None)
    with pytest.raises(BotoServerError):
        sqs.get_messages_from_queue()
    mock_obj.assert_called_once_with('us-west-2', **get_test_creds())


def test_delete_msg_no_exceptions(get_mock_boto):
    fake_conn = FakeConn()
    fake_queue = FakeQueue()
    fake_conn.set_queue(fake_queue)

    mock_obj = get_mock_boto
    mock_obj.return_value = fake_conn
    sqs = SQSWrapper("some-queue")
    test_msg = object()
    sqs.delete_message_from_queue(test_msg)
    mock_obj.assert_called_once_with('us-west-2', **get_test_creds())
    assert fake_queue.deleted_msg == test_msg


def test_delete_msg_batch(get_mock_boto):
    fake_conn = FakeConn()
    fake_queue = FakeQueue()
    fake_conn.set_queue(fake_queue)

    mock_obj = get_mock_boto
    mock_obj.return_value = fake_conn
    sqs = SQSWrapper("some-queue")
    test_msgs = [object()]
    sqs.delete_message_batch_from_queue(test_msgs)
    mock_obj.assert_called_once_with('us-west-2', **get_test_creds())
    assert fake_queue.deleted_msgs == test_msgs


def test_get_name(get_mock_boto):
    fake_conn = FakeConn()
    fake_queue = FakeQueue()
    fake_conn.set_queue(fake_queue)

    mock_obj = get_mock_boto
    mock_obj.return_value = fake_conn
    sqs = SQSWrapper("some-queue")
    assert sqs.get_queue_name() == "some-queue"


def test_clear(get_mock_boto):
    fake_conn = FakeConn()
    fake_queue = FakeQueue()
    fake_queue.set_messages([object()])
    fake_conn.set_queue(fake_queue)

    mock_obj = get_mock_boto
    mock_obj.return_value = fake_conn
    sqs = SQSWrapper("some-queue")
    sqs.clear()
    assert fake_queue.msgs is None
