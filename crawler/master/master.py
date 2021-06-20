#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors:      Manuel Martín Malagón
# Created:      2021/03/04
# Last update:  2021/05/16

import functools
import logging
import signal
import redis
import json
import time
import pika
import sys
import os

from neo4j import GraphDatabase, basic_auth, Session
from neo4j.work.transaction import Transaction
from pika.spec import PERSISTENT_DELIVERY_MODE
from pika.exchange_type import ExchangeType
from datetime import datetime
from uuid import uuid4

try:
    HOSTNAME = os.environ.get('HOSTNAME', str(uuid4()))

    AMQP_URL = os.environ.get('AMQP_URL', 'amqp://guest:guest@localhost:5672/%2F')

    PREFETCH_COUNT = int(os.environ.get('PREFETCH_COUNT', 1))

    REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379/0')

    # in seconds
    TIME_RUNNING = int(os.environ.get('TIME_RUNNING', 10800))
    TIME_IDLEING = int(os.environ.get('TIME_IDLEING', 18000))

    assert TIME_RUNNING > 0 and TIME_IDLEING > 0

    TOTAL_TIME = TIME_RUNNING + TIME_IDLEING

    DB_URL = os.environ.get('DB_URL', 'bolt://localhost:7687')
    DB_USR = os.environ.get('DB_USR', 'neo4j')
    DB_PWD = os.environ.get('DB_PWD', 'secret')

except:
    sys.exit(1)

PREFIX_URL = "/"

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


def handle_sigterm(*args):
    raise KeyboardInterrupt()


def _return_msg(hostname: str, timestamp: int, path: str):
    return {
        'hostname': hostname,
        'timestamp': timestamp,
        'path': path
    }


def _return_db(timestamp: int) -> str:
    return 't{timestamp}'.format(timestamp=timestamp)


class RealMaster(object):

    EXCHANGE = 'xenforo'
    EXCHANGE_TYPE = ExchangeType.topic
    
    INPUT_QUEUE = 'href'
    INPUT_ROUTING_KEY = 'xenforo.href'
    
    OUTPUT_QUEUE = 'url'
    OUTPUT_ROUTING_KEY = 'xenforo.url'

    BASIC_PROPERTIES = pika.BasicProperties(
        app_id=HOSTNAME, content_type='application/json',
        delivery_mode=PERSISTENT_DELIVERY_MODE)

    KNOWN_URLS = 'known_urls'


    def __init__(self):
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None

        self._closing = False
        self._consumer_tag = None

        self._consuming = False
        
        self._redis = None

        self._timestamp = None

        self._driver = None


    def _get_session(self, db_name: str = None) -> Session:
        if db_name:
            return self._driver.session(database=db_name)
        else: # default db
            return self._driver.session()


    @staticmethod
    def _tx_create_constraints(tx: Transaction):
        tx.run("CREATE CONSTRAINT member_id IF NOT EXISTS ON (m:Member) ASSERT (m.member_id) IS NODE KEY")
        tx.run("CREATE CONSTRAINT post_id IF NOT EXISTS ON (p:Post) ASSERT (p.post_id) IS NODE KEY")
        tx.run("CREATE CONSTRAINT thread_id IF NOT EXISTS ON (t:Thread) ASSERT (t.thread_id) IS NODE KEY")
        tx.run("CREATE CONSTRAINT forum_id IF NOT EXISTS ON (f:Forum) ASSERT (f.forum_id) IS NODE KEY")
        tx.run("CREATE CONSTRAINT attachment_id IF NOT EXISTS ON (a:Attachment) ASSERT (a.attachment_id) IS NODE KEY")
        tx.run("CREATE CONSTRAINT country_name IF NOT EXISTS ON (c:Country) ASSERT (c.name) IS NODE KEY")
        tx.run("CREATE CONSTRAINT banner_name IF NOT EXISTS ON (b:Banner) ASSERT (b.name) IS NODE KEY")
        tx.run("CREATE CONSTRAINT label_name IF NOT EXISTS ON (l:Label) ASSERT (l.name) IS NODE KEY")


    @staticmethod
    def _tx_create_database(tx: Transaction, db_name: str):
        tx.run("CREATE DATABASE $db_name IF NOT EXISTS", db_name=db_name)


    def _create_database(self, db_name: str):
        LOGGER.info('Creating %s database', db_name)
        # defaults to default database, which is 'neo4j' by default (so don't be a fool by deleting all databases or doing changes while executing)
        with self._get_session() as session:
            session.write_transaction(self._tx_create_database, db_name)

        with self._get_session(db_name) as session:
            session.write_transaction(self._tx_create_constraints)

        LOGGER.info('%s database successfully created!', db_name)

    def _get_timestamp(self) -> int:
        return int(self._redis.get('timestamp').decode('utf-8'))

    def _set_timestamp(self, timestamp):
        return self._redis.set('timestamp', timestamp)

    def _add_value_to_set(self, set, value):
        return self._redis.sadd(set, value)

    def _delete_set(self, set):
        return self._redis.delete(set)

    @staticmethod
    def _build_set_name(timestamp, suffix):
        return str(timestamp) + suffix

    def _initialization(self):
        LOGGER.info('Initializating/recovering master status...')
        try:
            self._timestamp = self._get_timestamp()
            diff = self._timestamp + TOTAL_TIME - int(datetime.now().timestamp())
            assert diff >= 0 and diff <= TOTAL_TIME
        except:
            diff = 0
        
        self.schedule_next_timestamp(diff)

    def connect(self):
        LOGGER.info('Connecting to %s', AMQP_URL)
        return pika.SelectConnection(
            parameters=pika.URLParameters(AMQP_URL),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        self._consuming = False
        if self._connection.is_closing or self._connection.is_closed:
            LOGGER.info('Connection is closing or already closed')
        else:
            LOGGER.info('Closing connection')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        LOGGER.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, _unused_err):
        LOGGER.error('Connection open failed')
        self.reconnect()

    def on_connection_closed(self, _unused_connection, _unused_reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reconnect necessary')
            self.reconnect()

    def reconnect(self):
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOGGER.info('Channel opened')
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange()

    def add_on_channel_close_callback(self):
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, _unused_channel, _unused_reason):
        LOGGER.warning('Channel was closed')
        self.close_connection()

    def setup_exchange(self):
        LOGGER.info('Declaring exchange: %s', self.EXCHANGE)
        self._channel.exchange_declare(
            exchange=self.EXCHANGE,
            exchange_type=self.EXCHANGE_TYPE,
            durable=True,
            callback=self.on_exchange_declareok)

    def on_exchange_declareok(self, _unused_frame):
        LOGGER.info('Exchange declared: %s', self.EXCHANGE)
        self.setup_queues()

    def setup_queues(self):
        LOGGER.info('Declaring output queue %s', self.OUTPUT_QUEUE)
        cb = functools.partial(self.on_queue_declareok, queue_name=self.OUTPUT_QUEUE,
                               routing_key=self.OUTPUT_ROUTING_KEY, is_publisher=True)
        self._channel.queue_declare(queue=self.OUTPUT_QUEUE, callback=cb, durable=True)

        LOGGER.info('Declaring input queue %s', self.INPUT_QUEUE)
        cb = functools.partial(self.on_queue_declareok, queue_name=self.INPUT_QUEUE,
                               routing_key=self.INPUT_ROUTING_KEY, is_publisher=False)
        self._channel.queue_declare(queue=self.INPUT_QUEUE, callback=cb, durable=True)

    def on_queue_declareok(self, _unused_frame, queue_name, routing_key, is_publisher):
        LOGGER.info('Binding %s to %s with %s', self.EXCHANGE, queue_name,
                    routing_key)
        cb = functools.partial(self.on_bindok, queue_name=queue_name,
                               is_publisher=is_publisher)
        self._channel.queue_bind(
            queue_name,
            self.EXCHANGE,
            routing_key=routing_key,
            callback=cb)

    def on_bindok(self, _unused_frame, queue_name, is_publisher):
        LOGGER.info('Queue bound: %s', queue_name)
        if not is_publisher:
            self.set_qos()

    def schedule_next_timestamp(self, publish_interval):
        diff = publish_interval - TIME_IDLEING
        if diff <= 0: diff, self._timestamp = 0, None # maybe self._timestamp=None not needed
        LOGGER.info('Scheduling purge queue for %0.1f seconds', diff)
        cb = functools.partial(self.purge_queue, publish_interval=(publish_interval-diff))
        self._connection.ioloop.call_later(diff, cb)

    def purge_queue(self, publish_interval):
        self._timestamp = None
        self._delete_set(self.KNOWN_URLS)
        cb = functools.partial(self.on_purge_queue, publish_interval=publish_interval)
        self._channel.queue_purge(self.OUTPUT_QUEUE, cb)

    def on_purge_queue(self, _unused_frame, publish_interval):
        LOGGER.info('Queue purged')
        self.schedule_next_message(publish_interval)

    def schedule_next_message(self, publish_interval):
        LOGGER.info('Scheduling next timestamp for %0.1f seconds', publish_interval)
        self._connection.ioloop.call_later(publish_interval, self.publish_new_timestamp)

    def publish_new_timestamp(self):
        self._timestamp = int(datetime.now().timestamp())
        self._set_timestamp(self._timestamp)
        self._create_database(_return_db(self._timestamp))
        self._add_value_to_set(self.KNOWN_URLS, PREFIX_URL)
        self.publish_message(_return_msg(hostname=HOSTNAME, timestamp=self._timestamp, path=PREFIX_URL))
        LOGGER.info('Published new timestamp: %d', self._timestamp)
        self.schedule_next_timestamp(TOTAL_TIME)

    def publish_message(self, message):
        if self._channel is None or not self._channel.is_open:
            return
        self._channel.basic_publish(
            self.EXCHANGE, self.OUTPUT_ROUTING_KEY,
            json.dumps(message), self.BASIC_PROPERTIES)
        LOGGER.info('Published message: %s', str(message))

    def set_qos(self):
        self._channel.basic_qos(
            prefetch_count=PREFETCH_COUNT, callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        LOGGER.info('QOS set to: %d', PREFETCH_COUNT)
        self.start_consuming()

    def start_consuming(self):
        LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._initialization()
        self._consumer_tag = self._channel.basic_consume(
            self.INPUT_QUEUE, self.on_message)
        self.was_consuming = True
        self._consuming = True

    def add_on_cancel_callback(self):
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, _unused_method_frame):
        LOGGER.info('Consumer was cancelled remotely, shutting down')
        if self._channel:
            self._channel.close()

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)
        try:
            data = json.loads(body)
            timestamp, href = data['timestamp'], data['href']

            urls_to_publish = []

            if timestamp == self._timestamp:
                for url in href:
                    result = self._add_value_to_set(self.KNOWN_URLS, url)
                    if result:
                        urls_to_publish.append(url)
        except:
            LOGGER.exception('Rejecting message')
            self.reject_message(basic_deliver.delivery_tag)
        else:
            for url in urls_to_publish: # if its timestamp isn't the correct one, nothing happens because list is empty
                self.publish_message(_return_msg(hostname=HOSTNAME, timestamp=timestamp, path=url))
            self.acknowledge_message(basic_deliver.delivery_tag)

    def acknowledge_message(self, delivery_tag):
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def reject_message(self, delivery_tag):
        LOGGER.info('Rejecting message %s', delivery_tag)
        self._channel.basic_reject(delivery_tag, False) # unenqueued because it's malformed

    def stop_consuming(self):
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            cb = functools.partial(
                self.on_cancelok, userdata=self._consumer_tag)
            self._channel.basic_cancel(self._consumer_tag, cb)

    def on_cancelok(self, _unused_frame, _unused_userdata):
        self._consuming = False
        LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self):
        try:
            self._redis = redis.from_url(REDIS_URL)
            self._redis.ping()
            self._driver = GraphDatabase.driver(uri=DB_URL, auth=basic_auth(DB_USR, DB_PWD), encrypted=False) # it does include TCP keep alive every 5 minutes (default server config.)
        except:
            LOGGER.info('Database connection error')
            self.should_reconnect = True
            self._closing = True
        else:
            self._connection = self.connect()
            self._connection.ioloop.start()

    def stop(self):
        if not self._closing:
            self._closing = True
            LOGGER.info('Stopping')
            if self._consuming:
                self.stop_consuming()
                self._connection.ioloop.start()
            else:
                self._connection.ioloop.stop()
            self._driver.close()
            LOGGER.info('Stopped')


class Master(object):

    def __init__(self):
        self._reconnect_delay = 0
        self._master = RealMaster()

    def run(self):
        while True:
            try:
                self._master.run()
            except KeyboardInterrupt:
                self._master.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._master.should_reconnect:
            self._master.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info('Reconnecting after %d seconds', reconnect_delay)
            time.sleep(reconnect_delay)
            self._master = RealMaster()

    def _get_reconnect_delay(self):
        if self._master.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    signal.signal(signal.SIGTERM, handle_sigterm)
    master = Master()
    master.run()


if __name__ == '__main__':
    main()
