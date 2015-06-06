"""blocking adapter test"""
import logging
import socket
import time
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import uuid

from forward_server import ForwardServer

import pika
from pika.adapters import blocking_connection
import pika.connection
import pika.exceptions


# Disable warning about access to protected member
# pylint: disable=W0212

# Disable warning Attribute defined outside __init__
# pylint: disable=W0201

# Disable warning Missing docstring
# pylint: disable=C0111

# Disable warning Too many public methods
# pylint: disable=R0904

# Disable warning Invalid variable name
# pylint: disable=C0103

LOGGER = logging.getLogger(__name__)
PARAMS_URL_TEMPLATE = (
    'amqp://guest:guest@127.0.0.1:%(port)s/%%2f?socket_timeout=1')
DEFAULT_URL = PARAMS_URL_TEMPLATE % {'port': 5672}
DEFAULT_PARAMS = pika.URLParameters(DEFAULT_URL)
DEFAULT_TIMEOUT = 15



class BlockingTestCase(unittest.TestCase):

    TIMEOUT = DEFAULT_TIMEOUT

    def _connect(self,
                 url=DEFAULT_URL,
                 connection_class=pika.BlockingConnection,
                 impl_class=None):
        parameters = pika.URLParameters(url)
        connection = connection_class(parameters, _impl_class=impl_class)
        self.addCleanup(lambda: connection.close()
                        if connection.is_open else None)

        connection._impl.add_timeout(
            self.TIMEOUT, # pylint: disable=E1101
            self._on_test_timeout)

        return connection

    def _on_test_timeout(self):
        """Called when test times out"""
        self.fail('Test timed out')


class TestCreateAndCloseConnection(BlockingTestCase):

    def start_test(self):
        """Create and close connection"""
        connection = self._connect()
        self.assertIsInstance(connection, pika.BlockingConnection)
        self.assertTrue(connection.is_open)
        self.assertFalse(connection.is_closed)
        self.assertFalse(connection.is_closing)

        connection.close()
        self.assertTrue(connection.is_closed)
        self.assertFalse(connection.is_open)
        self.assertFalse(connection.is_closing)


class TestSuddenBrokerDisconnectBeforeChannel(BlockingTestCase):

    def start_test(self):
        """BlockingConnection resets properly on TCP/IP drop during channel()
        """
        with ForwardServer((DEFAULT_PARAMS.host, DEFAULT_PARAMS.port)) as fwd:
            self.connection = self._connect(
                PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]})

        # Once outside the context, the connection is broken

        # BlockingConnection should raise ConnectionClosed
        with self.assertRaises(pika.exceptions.ConnectionClosed):
            self.connection.channel()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection._impl.socket)


class TestNoAccessToFileDescriptorAfterConnectionClosed(BlockingTestCase):

    def start_test(self):
        """BlockingConnection can't access file descriptor after
        ConnectionClosed
        """
        with ForwardServer((DEFAULT_PARAMS.host, DEFAULT_PARAMS.port)) as fwd:
            self.connection = self._connect(
                PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]})

        # Once outside the context, the connection is broken

        # BlockingConnection should raise ConnectionClosed
        with self.assertRaises(pika.exceptions.ConnectionClosed):
            self.connection.channel()

        self.assertTrue(self.connection.is_closed)
        self.assertFalse(self.connection.is_open)
        self.assertIsNone(self.connection._impl.socket)

        # Attempt to operate on the connection once again after ConnectionClosed
        self.assertIsNone(self.connection._impl.socket)
        with self.assertRaises(pika.exceptions.ConnectionClosed):
            self.connection.channel()


class TestConnectWithDownedBroker(BlockingTestCase):

    def start_test(self):
        """ BlockingConnection to downed broker results in AMQPConnectionError

        """
        # Reserve a port for use in connect
        sock = socket.socket()
        self.addCleanup(sock.close)

        sock.bind(("127.0.0.1", 0))

        port = sock.getsockname()[1]

        sock.close()

        with self.assertRaises(pika.exceptions.AMQPConnectionError):
            self.connection = self._connect(
                PARAMS_URL_TEMPLATE % {"port": port})


class TestDisconnectDuringConnectionStart(BlockingTestCase):

    def start_test(self):
        """ BlockingConnection TCP/IP connection loss in CONNECTION_START
        """
        fwd = ForwardServer((DEFAULT_PARAMS.host, DEFAULT_PARAMS.port))
        fwd.start()
        self.addCleanup(lambda: fwd.stop() if fwd.running else None)

        class MySelectConnection(pika.SelectConnection):
            assert hasattr(pika.SelectConnection, '_on_connection_start')

            def _on_connection_start(self, *args, **kwargs):
                fwd.stop()
                return super(MySelectConnection, self)._on_connection_start(
                    *args, **kwargs)

        with self.assertRaises(pika.exceptions.AMQPConnectionError) as cm:
            self._connect(
                PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]},
                impl_class=MySelectConnection)

            self.assertIsInstance(cm.exception,
                                  (pika.exceptions.ProbableAuthenticationError,
                                   pika.exceptions.ProbableAccessDeniedError))


class TestDisconnectDuringConnectionTune(BlockingTestCase):

    def start_test(self):
        """ BlockingConnection TCP/IP connection loss in CONNECTION_TUNE
        """
        fwd = ForwardServer((DEFAULT_PARAMS.host, DEFAULT_PARAMS.port))
        fwd.start()
        self.addCleanup(lambda: fwd.stop() if fwd.running else None)

        class MySelectConnection(pika.SelectConnection):
            assert hasattr(pika.SelectConnection, '_on_connection_tune')

            def _on_connection_tune(self, *args, **kwargs):
                fwd.stop()
                return super(MySelectConnection, self)._on_connection_tune(
                    *args, **kwargs)

        with self.assertRaises(pika.exceptions.ProbableAccessDeniedError):
            self._connect(
                PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]},
                impl_class=MySelectConnection)


class TestDisconnectDuringConnectionProtocol(BlockingTestCase):

    def start_test(self):
        """ BlockingConnection TCP/IP connection loss in CONNECTION_PROTOCOL
        """
        fwd = ForwardServer((DEFAULT_PARAMS.host, DEFAULT_PARAMS.port))
        fwd.start()
        self.addCleanup(lambda: fwd.stop() if fwd.running else None)

        class MySelectConnection(pika.SelectConnection):
            assert hasattr(pika.SelectConnection, '_on_connected')

            def _on_connected(self, *args, **kwargs):
                fwd.stop()
                return super(MySelectConnection, self)._on_connected(
                    *args, **kwargs)

        with self.assertRaises(pika.exceptions.IncompatibleProtocolError):
            self._connect(PARAMS_URL_TEMPLATE % {"port": fwd.server_address[1]},
                          impl_class=MySelectConnection)


class TestProcessDataEvents(BlockingTestCase):

    def start_test(self):
        """BlockingConnection.process_data_events"""
        connection = self._connect()

        # Try with time_limit=0
        start_time = time.time()
        connection.process_data_events(time_limit=0)
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.25)

        # Try with time_limit=0.005
        start_time = time.time()
        connection.process_data_events(time_limit=0.005)
        elapsed = time.time() - start_time
        self.assertGreaterEqual(elapsed, 0.005)
        self.assertLess(elapsed, 0.25)


class TestSleep(BlockingTestCase):

    def start_test(self):
        """BlockingConnection.sleep"""
        connection = self._connect()

        # Try with duration=0
        start_time = time.time()
        connection.sleep(duration=0)
        elapsed = time.time() - start_time
        self.assertLess(elapsed, 0.25)

        # Try with duration=0.005
        start_time = time.time()
        connection.sleep(duration=0.005)
        elapsed = time.time() - start_time
        self.assertGreaterEqual(elapsed, 0.005)
        self.assertLess(elapsed, 0.25)


class TestConnectionProperties(BlockingTestCase):

    def start_test(self):
        """Test BlockingConnection properties"""
        connection = self._connect()

        self.assertTrue(connection.is_open)
        self.assertFalse(connection.is_closing)
        self.assertFalse(connection.is_closed)

        self.assertTrue(connection.basic_nack_supported)
        self.assertTrue(connection.consumer_cancel_notify_supported)
        self.assertTrue(connection.exchange_exchange_bindings_supported)
        self.assertTrue(connection.publisher_confirms_supported)

        connection.close()
        self.assertFalse(connection.is_open)
        self.assertFalse(connection.is_closing)
        self.assertTrue(connection.is_closed)



class TestCreateAndCloseChannel(BlockingTestCase):

    def start_test(self):
        """Create and close channel"""
        connection = self._connect()

        ch = connection.channel()
        self.assertIsInstance(ch, blocking_connection.BlockingChannel)
        self.assertTrue(ch.is_open)
        self.assertFalse(ch.is_closed)
        self.assertFalse(ch.is_closing)
        self.assertIs(ch.connection, connection)

        ch.close()
        self.assertTrue(ch.is_closed)
        self.assertFalse(ch.is_open)
        self.assertFalse(ch.is_closing)


class TestExchangeDeclareAndDelete(BlockingTestCase):

    def start_test(self):
        """Test exchange_declare and exchange_delete"""
        connection = self._connect()

        ch = connection.channel()

        name = "TestExchangeDeclareAndDelete_" + uuid.uuid1().hex

        # Declare a new exchange
        frame = ch.exchange_declare(name, exchange_type='direct')
        self.addCleanup(connection.channel().exchange_delete, name)

        self.assertIsInstance(frame.method, pika.spec.Exchange.DeclareOk)

        # Check if it exists by declaring it passively
        frame = ch.exchange_declare(name, passive=True)
        self.assertIsInstance(frame.method, pika.spec.Exchange.DeclareOk)

        # Delete the exchange
        frame = ch.exchange_delete(name)
        self.assertIsInstance(frame.method, pika.spec.Exchange.DeleteOk)

        # Verify that it's been deleted
        with self.assertRaises(pika.exceptions.ChannelClosed) as cm:
            ch.exchange_declare(name, passive=True)

        self.assertEqual(cm.exception.args[0], 404)


class TestQueueDeclareAndDelete(BlockingTestCase):

    def start_test(self):
        """Test queue_declare and queue_delete"""
        connection = self._connect()

        ch = connection.channel()

        name = "TestQueueDeclareAndDelete_" + uuid.uuid1().hex

        # Declare a new queue
        frame = ch.queue_declare(name, auto_delete=True)
        self.addCleanup(connection.channel().queue_delete, name)

        self.assertIsInstance(frame.method, pika.spec.Queue.DeclareOk)

        # Check if it exists by declaring it passively
        frame = ch.queue_declare(name, passive=True)
        self.assertIsInstance(frame.method, pika.spec.Queue.DeclareOk)

        # Delete the queue
        frame = ch.queue_delete(name)
        self.assertIsInstance(frame.method, pika.spec.Queue.DeleteOk)

        # Verify that it's been deleted
        with self.assertRaises(pika.exceptions.ChannelClosed) as cm:
            ch.queue_declare(name, passive=True)

        self.assertEqual(cm.exception.args[0], 404)


class TestQueueBindAndUnbindAndPurge(BlockingTestCase):

    def start_test(self):
        """Test queue_bind and queue_unbind"""
        connection = self._connect()

        ch = connection.channel()

        q_name = 'TestQueueBindAndUnbindAndPurge_q' + uuid.uuid1().hex
        exg_name = 'TestQueueBindAndUnbindAndPurge_exg_' + uuid.uuid1().hex
        routing_key = 'TestQueueBindAndUnbindAndPurge'

        # Place channel in publisher-acknowledgments mode so that we may test
        # whether the queue is reachable by publishing with mandatory=True
        res = ch.confirm_delivery()
        self.assertIsNone(res)

        # Declare a new exchange
        ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCleanup(connection.channel().exchange_delete, exg_name)

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(connection.channel().queue_delete, q_name)

        # Bind the queue to the exchange using routing key
        frame = ch.queue_bind(q_name, exchange=exg_name,
                              routing_key=routing_key)
        self.assertIsInstance(frame.method, pika.spec.Queue.BindOk)

        # Check that the queue is empty
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)

        # Deposit a message in the queue
        ch.publish(exg_name, routing_key, body='TestQueueBindAndUnbindAndPurge',
                   mandatory=True)

        # Check that the queue now has one message
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 1)

        # Unbind the queue
        frame = ch.queue_unbind(queue=q_name, exchange=exg_name,
                                routing_key=routing_key)
        self.assertIsInstance(frame.method, pika.spec.Queue.UnbindOk)

        # Verify that the queue is now unreachable via that binding
        with self.assertRaises(blocking_connection.UnroutableError):
            ch.publish(exg_name, routing_key,
                       body='TestQueueBindAndUnbindAndPurge-2',
                       mandatory=True)

        # Purge the queue and verify that 1 message was purged
        frame = ch.queue_purge(q_name)
        self.assertIsInstance(frame.method, pika.spec.Queue.PurgeOk)
        self.assertEqual(frame.method.message_count, 1)

        # Verify that the queue is now empty
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)


class TestBasicGet(BlockingTestCase):

    def start_test(self):
        """BlockingChannel.basic_get"""
        connection = self._connect()

        ch = connection.channel()

        q_name = 'TestBasicGet_q' + uuid.uuid1().hex

        # Place channel in publisher-acknowledgments mode so that the message
        # may be delivered synchronously to the queue by publishing it with
        # mandatory=True
        ch.confirm_delivery()

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(connection.channel().queue_delete, q_name)

        # Verify result of getting a message from an empty queue
        msg = ch.basic_get(q_name, no_ack=False)
        self.assertTupleEqual(msg, (None, None, None))

        # Deposit a message in the queue via default exchange
        ch.publish(exchange='', routing_key=q_name, body='TestBasicGet',
                   mandatory=True)

        # Get the message
        (method, properties, body) = ch.basic_get(q_name, no_ack=False)
        self.assertIsInstance(method, pika.spec.Basic.GetOk)
        self.assertEqual(method.delivery_tag, 1)
        self.assertFalse(method.redelivered)
        self.assertEqual(method.exchange, '')
        self.assertEqual(method.routing_key, q_name)
        self.assertEqual(method.message_count, 0)

        self.assertIsInstance(properties, pika.BasicProperties)
        self.assertIsNone(properties.headers)
        self.assertEqual(body, 'TestBasicGet')

        # Ack it
        ch.basic_ack(delivery_tag=method.delivery_tag)

        # Verify that the queue is now empty
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)


class TestPublishAndConsumeAndQos(BlockingTestCase):

    def start_test(self):
        """BlockingChannel.basic_publish, publish, get_event, has_event, QoS"""
        connection = self._connect()

        ch = connection.channel()

        q_name = 'TestPublishAndConsumeAndQos_q' + uuid.uuid1().hex
        exg_name = 'TestPublishAndConsumeAndQos_exg_' + uuid.uuid1().hex
        routing_key = 'TestPublishAndConsumeAndQos'

        # Place channel in publisher-acknowledgments mode so that publishing
        # with mandatory=True will be synchronous
        res = ch.confirm_delivery()
        self.assertIsNone(res)

        # Declare a new exchange
        ch.exchange_declare(exg_name, exchange_type='direct')
        self.addCleanup(connection.channel().exchange_delete, exg_name)

        # Declare a new queue
        ch.queue_declare(q_name, auto_delete=True)
        self.addCleanup(connection.channel().queue_delete, q_name)

        # Verify unroutable message handling using basic_publish
        res = ch.basic_publish(exg_name, routing_key=routing_key, body='',
                               mandatory=True)
        self.assertEqual(res, False)

        # Verify unroutable message handling using publish
        with self.assertRaises(blocking_connection.UnroutableError) as cm:
            ch.publish(exg_name, routing_key=routing_key, body='',
                       mandatory=True)
        (msg,) = cm.exception.messages
        self.assertIsInstance(msg, blocking_connection.ReturnedMessage)
        self.assertIsInstance(msg.method, pika.spec.Basic.Return)
        self.assertEqual(msg.method.reply_code, 312)
        self.assertEqual(msg.method.exchange, exg_name)
        self.assertEqual(msg.method.routing_key, routing_key)
        self.assertIsInstance(msg.properties, pika.BasicProperties)
        self.assertEqual(msg.body, '')

        # Bind the queue to the exchange using routing key
        frame = ch.queue_bind(q_name, exchange=exg_name,
                              routing_key=routing_key)

        # Deposit a message in the queue via basic_publish
        res = ch.basic_publish(exg_name, routing_key=routing_key,
                               body='via-basic_publish',
                               mandatory=False)
        self.assertEqual(res, True)

        # Deposit another message in the queue via publish
        ch.publish(exg_name, routing_key, body='via-publish',
                   mandatory=True)

        # Check that the queue now has two messages
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 2)

        # Configure QoS for one message
        ch.basic_qos(prefetch_size=0, prefetch_count=1, all_channels=False)

        # Create a consumer
        consumer_tag = ch.create_consumer(q_name, no_ack=False,
                                          exclusive=False,
                                          arguments=None)

        # Test has_event() by waiting for first message to arrive
        while not ch.has_event():
            connection.process_data_events(time_limit=0)

        # Get the first message
        msg = ch.get_event()
        self.assertIsInstance(msg, blocking_connection.ConsumerDeliveryEvt)
        self.assertIsInstance(msg.method, pika.spec.Basic.Deliver)
        self.assertEqual(msg.method.consumer_tag, consumer_tag)
        self.assertEqual(msg.method.delivery_tag, 1)
        self.assertFalse(msg.method.redelivered)
        self.assertEqual(msg.method.exchange, exg_name)
        self.assertEqual(msg.method.routing_key, routing_key)

        self.assertIsInstance(msg.properties, pika.BasicProperties)
        self.assertEqual(msg.body, 'via-basic_publish')

        # There shouldn't be any more events now
        self.assertFalse(ch.has_event())

        # Ack the mesage so that the next one can arrive (we configured QoS with
        # prefetch_count=1)
        ch.basic_ack(delivery_tag=msg.method.delivery_tag, multiple=False)

        # Get the second message
        msg = ch.get_event()
        self.assertIsInstance(msg, blocking_connection.ConsumerDeliveryEvt)
        self.assertIsInstance(msg.method, pika.spec.Basic.Deliver)
        self.assertEqual(msg.method.consumer_tag, consumer_tag)
        self.assertEqual(msg.method.delivery_tag, 2)
        self.assertFalse(msg.method.redelivered)
        self.assertEqual(msg.method.exchange, exg_name)
        self.assertEqual(msg.method.routing_key, routing_key)

        self.assertIsInstance(msg.properties, pika.BasicProperties)
        self.assertEqual(msg.body, 'via-publish')

        # There shouldn't be any more events now
        self.assertFalse(ch.has_event())

        ch.basic_ack(delivery_tag=msg.method.delivery_tag, multiple=False)

        # Verify that the queue is now empty
        frame = ch.queue_declare(q_name, passive=True)
        self.assertEqual(frame.method.message_count, 0)

        # Attempt get_event again with a short timeout
        res = ch.get_event(inactivity_timeout=0.005)
        self.assertIsNone(res)

        # Delete the queue to force consumer cancellation
        ch.queue_delete(q_name)

        # Receive consumer cancellation
        evt = ch.get_event(inactivity_timeout=None)
        self.assertIsInstance(evt, blocking_connection.ConsumerCancellationEvt)
        self.assertEqual(evt.consumer_tag, consumer_tag)






