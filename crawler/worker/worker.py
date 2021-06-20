#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Authors:      Manuel Martín Malagón
# Created:      2021/03/04
# Last update:  2021/05/21

import functools
import requests
import logging
import random
import signal
import json
import time
import pika
import sys
import os
import re

from neo4j import GraphDatabase, basic_auth, Session
from neo4j.work.transaction import Transaction
from pika.spec import PERSISTENT_DELIVERY_MODE
from pika.exchange_type import ExchangeType
from neo4j.exceptions import ClientError, ConstraintError
from requests.models import Response
from http.client import responses
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from uuid import uuid4

try:
    HOSTNAME = os.environ.get('HOSTNAME', str(uuid4()))

    AMQP_URL = os.environ.get('AMQP_URL', 'amqp://guest:guest@localhost:5672/%2F')

    PREFETCH_COUNT = int(os.environ.get('PREFETCH_COUNT', 1))

    NUM_RETRIES = int(os.environ.get('NUM_RETRIES', 3))

    BASE_URL = os.environ.get('BASE_URL')
    USER_AGENT = os.environ.get('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36')

    TIMEOUT = int(os.environ.get('TIMEOUT', 20))

    DB_URL = os.environ.get('DB_URL', 'bolt://localhost:7687')
    DB_USR = os.environ.get('DB_USR', 'neo4j')
    DB_PWD = os.environ.get('DB_PWD', 'secret')

except:
    sys.exit(1)

BASE_RE = re.compile('^/$')
MEMBERS_RE = re.compile('^/members/[^/ ]+\.[0-9]+/$')
LOCATION_RE = re.compile('^/misc/location-info\?location=*')
ATTACHMENTS_RE = re.compile('^/attachments/[^/ ]+\.[0-9]+/$') # this is used only while scrapping
FORUMS_RE = re.compile('^/forums/[^/ ]+\.[0-9]+/(?:page-[0-9]+)?$')
THREADS_RE = re.compile('^/threads/[^/ ]+\.[0-9]+/(?:page-[0-9]+)?$')
LIKES_RE = re.compile('^/posts/[0-9]+/reactions(?:\?reaction_id=0&list_only=1&page=[0-9]+)?$')

XENFORO_RE = [BASE_RE, MEMBERS_RE, FORUMS_RE, THREADS_RE, LIKES_RE]

PREFIX_URL = "/"
HEADERS = {'User-Agent': USER_AGENT}

PARSER = 'lxml'

UNITS = {"BYTES": 1, "KB": 10**3, "MB": 10**6, "GB": 10**9, "TB": 10**12, "PB": 10**15, "EB": 10**18, "ZB": 10**21, "YB": 10**24}


def handle_sigterm(*args):
    raise KeyboardInterrupt()


def _parse_size(size: str) -> int:
    try:
        number, unit = [string.strip() for string in size.split()]
        return int(float(number)*UNITS[unit.upper()])
    except: # fallback
        return 0


def _return_msg(hostname: str, timestamp: int, path: str, status_code: int = -1, message: str = 'Exception', href: list = [], payload = None) -> dict:
    return {
        'hostname': hostname,
        'timestamp': timestamp,
        'path': path,
        'int': status_code,
        'message': message,
        'href': href,
        'payload': payload
    }


def _return_db(timestamp: int) -> str:
    return 't{timestamp}'.format(timestamp=timestamp)


def _cleaned_text(text: str) -> str:
    return ' '.join(text.split())


def _cleaned_number_text(text: str) -> str:
    return _cleaned_text(text).replace(',','')


def _cleaned_parentheses_number_text(text: str) -> str:
    return _cleaned_number_text(text).replace('(','').replace(')','')


def _get_ratio(text: str) -> float:
    try:
        return float(text)
    except:
        return None


def _path_id(path: str) -> int:
    if not BASE_RE.match(path):
        return int(path.split('/')[2].split('.')[-1])
    else:
        return None


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class RealWorker(object):

    EXCHANGE = 'xenforo'
    EXCHANGE_TYPE = ExchangeType.topic
    
    DATABASE_QUEUE = 'database'
    DATABASE_ROUTING_KEY = 'xenforo.database'

    OUTPUT_QUEUE = 'href'
    OUTPUT_ROUTING_KEY = 'xenforo.href'
    
    INPUT_QUEUE = 'url'
    INPUT_ROUTING_KEY = 'xenforo.url'

    BASIC_PROPERTIES = pika.BasicProperties(
        app_id=HOSTNAME, content_type='application/json',
        delivery_mode=PERSISTENT_DELIVERY_MODE)


    def __init__(self):
        self.should_reconnect = False
        self.was_consuming = False

        self._connection = None
        self._channel = None

        self._closing = False
        self._consumer_tag = None

        self._consuming = False
        
        self._prev_path = None
        self._prev_token = None

        self._driver = None


    def _get_session(self, db_name: str = None) -> Session:
        if db_name:
            return self._driver.session(database=db_name)
        else: # default db
            return self._driver.session()


    @staticmethod
    def _tx_member(tx: Transaction, member: dict):
        tx.run("MERGE (m:Member {member_id: $member_id}) SET m.username = $username, m.downloaded = $downloaded, m.uploaded = $uploaded, " +
            "m.ratio = $ratio, m.seedbonus = $seedbonus, m.current_upload = $current_upload, m.total_upload = $total_upload, " +
            "m.joined = $joined, m.last_seen = $last_seen",
            member_id=member['member_id'], username=member['username'], downloaded=member['downloaded'], uploaded=member['uploaded'],
            ratio=member['ratio'], seedbonus=member['seedbonus'], current_upload=member['current_upload'], total_upload=member['total_upload'],
            joined=member.get('joined'), last_seen=member.get('last_seen'))


    @staticmethod
    def _tx_post(tx: Transaction, post: dict, thread_id: int):
        _member_id = post['member']['member_id'] if post['member']['member_id'] != 0 else ("[DELETED]" + post['member']['username'])
        tx.run("MERGE (p:Post {post_id: $post_id}) SET p.text = $text, p.creation_date = $creation_date, p.modified_date = $modified_date " +
            "MERGE (t:Thread {thread_id: $thread_id}) MERGE (t)-[:CONTAINER_OF]->(p) " +
            "MERGE (m:Member {member_id: $member_id}) ON CREATE SET m.username = $username MERGE (p)-[:HAS_CREATOR]->(m)",
            post_id=post['post_id'], text=post['text'], creation_date=post['creation_date'], modified_date=post['modified_date'],
            thread_id=thread_id, member_id=_member_id, username=post['member']['username'])


    @staticmethod
    def _tx_thread(tx: Transaction, thread: dict, forum_id: int):
        tx.run("MERGE (t:Thread {thread_id: $thread_id}) SET t.title = $title " +
            "MERGE (f:Forum {forum_id: $forum_id}) MERGE (f)-[:CONTAINER_OF]->(t)",
            thread_id=thread['thread_id'], title=thread['title'], forum_id=forum_id)


    @staticmethod
    def _tx_forum(tx: Transaction, forum: dict, contained_id: int):
        tx.run("MERGE (f:Forum {forum_id: $forum_id}) SET f.title = $title, f.description = $description " +
            "MERGE (c:Forum {forum_id: $contained_id}) MERGE (c)-[:CONTAINER_OF]->(f)",
            forum_id=forum['forum_id'], title=forum['title'], description=forum['description'], contained_id=contained_id)


    @staticmethod
    def _tx_attachment(tx: Transaction, post_id: int, attachment: dict):
        tx.run("MERGE (a:Attachment {attachment_id: $attachment_id}) SET a.filename = $filename " +
            "MERGE (p:Post {post_id: $post_id}) MERGE (p)-[:HAS_ATTACHMENT]->(a)",
            attachment_id=attachment['attachment_id'], filename=attachment['filename'], post_id=post_id)


    @staticmethod
    def _tx_reply_of(tx: Transaction, post_id: int, reply_of: int):
        tx.run("MERGE (p:Post {post_id: $post_id}) MERGE (o:Post {post_id: $reply_of}) MERGE (p)-[:REPLY_OF]->(o)",
            post_id=post_id, reply_of=reply_of)


    @staticmethod
    def _tx_label(tx: Transaction, thread_id: int, label: str):
        tx.run("MERGE (t:Thread {thread_id: $thread_id}) MERGE (l:Label {name: $name}) MERGE (t)-[:HAS_LABEL]->(l)",
            thread_id=thread_id, name=label)


    @staticmethod
    def _tx_banner(tx: Transaction, member_id: int, banner: str):
        tx.run("MERGE (m:Member {member_id: $member_id}) MERGE (b:Banner {name: $name}) MERGE (m)-[:HAS_BANNER]->(b)",
            member_id=member_id, name=banner)


    @staticmethod
    def _tx_country(tx: Transaction, member_id: int, country: str):
        tx.run("MERGE (m:Member {member_id: $member_id}) MERGE (c:Country {name: $name}) MERGE (m)-[:IS_PLACED_IN]->(c)",
            member_id=member_id, name=country)


    @staticmethod
    def _tx_likes(tx: Transaction, post_id: int, member_id: int, username: str):
        _member_id = member_id if member_id != 0 else ("[DELETED]" + username)
        tx.run("MERGE (m:Member {member_id: $member_id}) ON CREATE SET m.username = $username MERGE (p:Post {post_id: $post_id}) MERGE (m)-[:LIKES]->(p)",
            member_id=_member_id, username=username, post_id=post_id)


    @staticmethod
    def _tx_thread_dict(tx: Transaction, thread: dict):
        for post in thread['posts']:
            RealWorker._tx_post(tx, post, thread['thread_id'])
            for reply_of in post['reply_of']:
                RealWorker._tx_reply_of(tx, post['post_id'], reply_of)
            for attachment in post['attachments']:
                RealWorker._tx_attachment(tx, post['post_id'], attachment)


    @staticmethod
    def _tx_forum_dict(tx: Transaction, forum: dict):
        for embedded_forum in forum['forums']:
            RealWorker._tx_forum(tx, embedded_forum, forum['forum_id'])
        for embedded_thread in forum['threads']:
            RealWorker._tx_thread(tx, embedded_thread, forum['forum_id'])
            for label in embedded_thread['labels']:
                RealWorker._tx_label(tx, embedded_thread['thread_id'], label)


    @staticmethod
    def _tx_member_dict(tx: Transaction, member: dict):
        RealWorker._tx_member(tx, member)
        for banner in member['banners']:
            RealWorker._tx_banner(tx, member['member_id'], banner)
        if 'country' in member:
            RealWorker._tx_country(tx, member['member_id'], member['country'])


    @staticmethod
    def _tx_likes_dict(tx: Transaction, post: dict):
        for member in post['members']:
            RealWorker._tx_likes(tx, post['post_id'], member['member_id'], member['username'])


    @staticmethod
    def _tx_base_list(tx: Transaction, base: list):
        for forum in base:
            RealWorker._tx_forum(tx, forum, 0)
            for embedded_forum in forum['forums']:
                RealWorker._tx_forum(tx, embedded_forum, forum['forum_id'])


    def _save_transaction(self, db_name, tx_function, payload):
        LOGGER.info('Saving transaction')
        with self._get_session(db_name) as session:
            session.write_transaction(tx_function, payload)
        LOGGER.info('Successfully saved transaction')


    def _save_payload_data(self, payload, db_name, tx_function):
        try:
            self._save_transaction(db_name, tx_function, payload)

        except ConstraintError as e:
            if e.code == 'Neo.ClientError.Schema.ConstraintValidationFailed':
                LOGGER.warning(e.code)
                time.sleep(random.uniform(1.0, 2.0))
                self._save_transaction(db_name, tx_function, payload)
            else:
                raise e


    def _reset_web_session(self):
        self._web_session = requests.Session()
        self._web_session.headers.update(HEADERS)


    def _update_token(self, path: str, web_soup: BeautifulSoup):
        try:
            self._prev_path, self._prev_token = path, web_soup.html['data-csrf']
        except:
            self._prev_path, self._prev_token = None, None


    @staticmethod
    def _thread(web_soup: BeautifulSoup, path_id: int):
        thread = {}

        # THREAD INFO
        # thread['thread_id'] = int(web_soup.select_one('.block-container')['data-lb-id'].split('-')[-1]) # div.block-container.lbContainer
        thread['thread_id'] = path_id # int(web_soup.html['data-content-key'].split('-')[-1])

        # THREAD POSTS
        thread['posts'] = []

        for item in web_soup.select('.message'): # article
            # EACH THREAD POST
            post = {}

            post['post_id'] = int(item['data-content'].split('-')[-1])
            post['creation_date'] = item.select_one('.message-attribution-main').select_one('time')['datetime'] # div
            
            last = item.select_one('.message-lastEdit')
            post['modified_date'] = last.select_one('time')['datetime'] if last else None # div

            content = item.select_one('.message-userContent') # div
            # this is for quote boxes which have unwanted text (from a drop-down menu)
            # div (extract or decompose, I need to look at it)
            [x.extract() for x in content.select('.bbCodeBlock-expandLink')]
            # this is for hidden URLs which are unavailable to unregistered users, so let's clean-up a little
            [x.extract() for x in content.select('.messageHide--link')] # div
                    
            post['text'] = _cleaned_text(content.select_one('.bbWrapper').text) # div
            post['reply_of'] = [int(quote['data-content-selector'].split('-')[-1])
                                for quote in content.select('.bbCodeBlock-sourceJump')] # a
            post['attachments'] = [{
                'attachment_id': _path_id(attachment['href']),
                'filename': _cleaned_text(attachment.text)
            } for attachment in item.find_all(href=ATTACHMENTS_RE)]

            user = item.select_one('.message-userDetails').select_one('.username') # div # span

            post['member'] = {
                'member_id': int(user['data-user-id']),
                'username' : _cleaned_text(user.text)
            }

            thread['posts'].append(post)

        return thread
        

    @staticmethod
    def _forum(web_soup: BeautifulSoup, path_id: int):
        forum = {}

        forum['forum_id'] = path_id # int(web_soup.html['data-container-key'].split('-')[-1])
                
        forum['forums'] = []
        forum['threads'] = []

        for forum_web in web_soup.select('.node'): # div
            local_forum = {}

            node_title = forum_web.select_one('.node-title') # h3

            local_forum['forum_id'] = _path_id(node_title.find(href=FORUMS_RE)['href']) # a
            local_forum['title'] = _cleaned_text(node_title.text) # div

            description = forum_web.select_one('.node-description')
            local_forum['description'] = _cleaned_text(description.text) if description else None

            forum['forums'].append(local_forum)

        for thread_web in web_soup.select('.structItem'): # div
            local_thread = {}

            struct_item_title = thread_web.select_one('.structItem-title')
            if struct_item_title:
                non_label_title = struct_item_title.find(href=THREADS_RE) # a

                local_thread['thread_id'] = _path_id(non_label_title['href']) # a
                local_thread['title'] = _cleaned_text(non_label_title.text)
                local_thread['labels'] = [_cleaned_text(label.text) for label in struct_item_title.select('.label')] # span

                forum['threads'].append(local_thread)

        return forum


    @staticmethod
    def _member(web_soup: BeautifulSoup, path_id: int):
        member = {}

        member['member_id'] = path_id # int(name['data-user-id'])
        member['username'] = _cleaned_text(web_soup.select_one('.username').text)
        member['banners'] = [_cleaned_text(banner.text) for banner in web_soup.select('.userBanner')]
        member['downloaded'] = _parse_size(_cleaned_number_text(web_soup.select_one('.torrentUserDownloaded').text))
        member['uploaded'] = _parse_size(_cleaned_number_text(web_soup.select_one('.torrentUserUploaded').text))
        member['ratio'] = _get_ratio(_cleaned_number_text(web_soup.select_one('.torrentUserRatio').text))
        member['seedbonus'] = int(_cleaned_number_text(web_soup.select_one('.torrentUserSeedbonus').text))
        member['current_upload'] = int(_cleaned_number_text(web_soup.select_one('.current_upload_count').text))
        member['total_upload'] = int(_cleaned_parentheses_number_text(web_soup.select_one('.total_upload_count').text))

        for blurb in web_soup.select('.memberTooltip-blurb'):
            location = blurb.find(href=LOCATION_RE)
            time = blurb.select_one('time')
            if location:
                member['country'] = _cleaned_text(location.text).upper()
            elif time:
                text = blurb.text
                if 'Joined' in text:
                    member['joined'] = time['datetime']
                elif 'Last seen' in text:
                    member['last_seen'] = time['datetime']
        
        return member


    @staticmethod
    def _likes(web_soup: BeautifulSoup, path_id: int):
        post = {}

        post['post_id'] = path_id
        post['members'] = []
        
        for row in web_soup.select('.contentRow-header'):
            like = {}

            username = row.select_one('.username')

            like['member_id'] = int(username['data-user-id'])
            like['username'] = _cleaned_text(username.text)

            post['members'].append(like)

        return post


    @staticmethod
    def _base(web_soup: BeautifulSoup, path_id: int):
        forums = []

        for block in web_soup.select_one('li#home').select('.block'):
            forum = {}

            forum['forum_id'] = int(block.select_one('.u-anchorTarget')['id'].split('.')[-1]) # span

            header = block.select_one('.block-header')
            desc = header.select_one('.block-desc') # span
            forum['description'] = _cleaned_text(desc.text) if desc else None

            [x.extract() for x in header.find_all(recursive=False)]
            forum['title'] = _cleaned_text(header.text)

            forum['forums'] = []
            for node in block.select('.node'):
                local_forum = {}

                href = node.select_one('.node-title').find(href=FORUMS_RE)

                local_forum['forum_id'] = _path_id(href['href'])
                local_forum['title'] = _cleaned_text(href.text)

                # title = node.select_one('.node-title')
                description = node.select_one('.node-description')
                local_forum['description'] = _cleaned_text(description.text) if description else None

                forum['forums'].append(local_forum)
            
            forums.append(forum)

        return forums


    def _selector(self, path: str) -> tuple:
        if THREADS_RE.match(path):
            return (self._thread, self._tx_thread_dict, False)
        elif FORUMS_RE.match(path):
            return (self._forum, self._tx_forum_dict, False)
        elif MEMBERS_RE.match(path):
            return (self._member, self._tx_member_dict, True) # it's member
        elif LIKES_RE.match(path):
            return (self._likes, self._tx_likes_dict, False)
        elif BASE_RE.match(path):
            return (self._base, self._tx_base_list, False)
        else:
            raise Exception('Incorrect selected tuple from path "{p}". Maybe a bug or system security has been compromised?', p=path)


    def _basic_request(self, path: str) -> Response:
        url = urljoin(BASE_URL, path)

        retry, ok = 0, False
        exception = None

        while not ok and retry < NUM_RETRIES:
            try:
                web_request = self._web_session.get(url, timeout=TIMEOUT)
                web_request.raise_for_status()
            except Exception as e:
                exception = e
                retry += 1
                time.sleep(retry) # sleep some seconds to avoid: [Errno -2] Name does not resolve
            else:
                ok = True

        if ok:
            return web_request
        else:
            raise exception


    def _parametrized_members_url(self, path: str) -> str:
        if self._prev_path is None or self._prev_token is None:
            self._update_token(PREFIX_URL, BeautifulSoup(self._basic_request(PREFIX_URL).text, PARSER))

        if self._prev_path is not None and self._prev_token is not None:
            return '{path}?tooltip=true&_xfRequestUri={prev_path}&_xfWithData=1&_xfToken={prev_token}&_xfResponseType=json'.format(
                path=path, prev_path=self._prev_path, prev_token=self._prev_token)
        else: # if there's no token, let's just throw an exception because it's not expected
            raise Exception('There\'s no token available for members request. Skipping...')


    def _request(self, path: str, timestamp: int) -> dict:
        try:
            selection = self._selector(path)

            web_request = self._basic_request(path if not selection[2] else self._parametrized_members_url(path))
            web_soup = BeautifulSoup(web_request.text if not selection[2] else web_request.json()['html']['content'], PARSER)

            # for every request that isn't MEMBERS_RE, we store new values into global-object variables
            if not selection[2]: self._update_token(path, web_soup)

            href = [_href['href'] for _href in web_soup.find_all(href=True) if any(_re.match(_href['href']) for _re in XENFORO_RE)]

            payload = selection[0](web_soup, _path_id(path))

            # self._save_payload_data(selection[0](web_soup, _path_id(path)), _return_db(timestamp), selection[1])

        except Exception as e:
            error = e.message if hasattr(e, 'message') else str(e)
            LOGGER.error(error)
            self._reset_web_session()
            return _return_msg(hostname=HOSTNAME, timestamp=timestamp, path=path,
                status_code=web_request.status_code if 'web_request' in locals() and hasattr(web_request, 'status_code') else -1,
                message=error, href=href if 'href' in locals() else [], payload=None)
        
        else:
            return _return_msg( hostname=HOSTNAME, timestamp=timestamp, path=path, status_code=web_request.status_code,
                message=responses[web_request.status_code], href=href, payload=(payload, _return_db(timestamp), selection[1]))

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
            deserialized_body = json.loads(body)
            path, timestamp = deserialized_body['path'], deserialized_body['timestamp']
            result = self._request(path, timestamp)
        except:
            LOGGER.exception('Rejecting message')
            self.reject_message(basic_deliver.delivery_tag) # it's malformed due to missing mandatory key-value pairs
        else:
            payload = result.pop('payload')
            if payload is not None: self._save_payload_data(payload[0], payload[1], payload[2])
            self.publish_message(result)
            self.acknowledge_message(basic_deliver.delivery_tag) # even if it fails by downloading, scraping, etc, will be acked because it's supposed to be good

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
            self._driver = GraphDatabase.driver(uri=DB_URL, auth=basic_auth(DB_USR, DB_PWD), encrypted=False) # it does include TCP keep alive every 5 minutes (default server config.)
        except:
            LOGGER.info('Database driver connection error')
            self.should_reconnect = True
            self._closing = True
        else:
            self._reset_web_session()
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


class Worker(object):

    def __init__(self):
        self._reconnect_delay = 0
        self._worker = RealWorker()

    def run(self):
        while True:
            try:
                self._worker.run()
            except KeyboardInterrupt:
                self._worker.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._worker.should_reconnect:
            self._worker.stop()
            reconnect_delay = self._get_reconnect_delay()
            LOGGER.info('Reconnecting after %d seconds', reconnect_delay)
            time.sleep(reconnect_delay)
            self._worker = RealWorker()

    def _get_reconnect_delay(self):
        if self._worker.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    signal.signal(signal.SIGTERM, handle_sigterm)
    worker = Worker()
    worker.run()


if __name__ == '__main__':
    main()
