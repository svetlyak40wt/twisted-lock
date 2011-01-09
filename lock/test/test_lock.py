# -*- coding: utf-8 -*-
from __future__ import absolute_import

import re

from twisted.trial import unittest
from twisted.web.client import Agent
from twisted.internet import reactor
from twisted.internet.base import DelayedCall
from twisted.internet.task import deferLater
from twisted.internet.defer import inlineCallbacks, gatherResults
from twisted.web.http import EXPECTATION_FAILED, OK
from StringIO import StringIO

from .. lock import LockFactory, LockProtocol
from .. config import Config
from .. utils import init_logging

DelayedCall.debug = True

def cfg(text):
    config = Config()
    config.readfp(StringIO(text))
    return config


class TestCase(unittest.TestCase):
    num_nodes = 3

    def __init__(self, *args, **kwargs):
        init_logging()
        self.client = Agent(reactor)

        base_cfg = '''
[cluster]
nodes = %s
[myself]
listen = %s
[web]
listen = %s
'''
        nodes = ', '.join(
            '127.0.0.1:%s' % (4001 + x)
            for x in range(self.num_nodes)
        )
        self.configs = [
            cfg(base_cfg % (nodes, 4001 + x, 9001 + x))
            for x in range(self.num_nodes)
        ]

        super(TestCase, self).__init__(*args, **kwargs)


    def setUp(self):
        LockProtocol.send_line_hooks = []
        self.servers = []

        for x, cfg in enumerate(self.configs):
            s = LockFactory(cfg)
            setattr(self, 's%s' % (x+1), s)
            self.servers.append(s)
            self.addCleanup(s.close)

    @inlineCallbacks
    def wait_when_connection_establied(self):
        yield gatherResults([s.when_connected() for s in self.servers])


class Simple(TestCase):
    num_nodes = 1
    @inlineCallbacks
    def test_start_server(self):
        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah' % (self.s1.http_interface, self.s1.http_port)
        )
        self.assertEqual(EXPECTATION_FAILED, result.code)


class Replication(TestCase):
    @inlineCallbacks
    def test_node_become_a_master(self):
        yield self.wait_when_connection_establied()
        server = self.s1
        self.assertEqual(None, server.master)
        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah' % (server.http_interface, server.http_port)
        )
        self.assertEqual((server.interface, server.port), server.master)


    @inlineCallbacks
    def test_data_replicated_to_all_nodes(self):
        yield self.wait_when_connection_establied()

        for s in self.servers:
            self.assertEqual({}, s._keys)

        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah' % (self.s1.http_interface, self.s1.http_port)
        )
        for s in self.servers:
            self.assertEqual({'blah': ''}, s._keys)


    @inlineCallbacks
    def test_data_deletion_replicated_too(self):
        yield self.wait_when_connection_establied()

        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah' % (self.s1.http_interface, self.s1.http_port)
        )
        for s in self.servers:
            self.assertEqual({'blah': ''}, s._keys)

        result = yield self.client.request(
            'DELETE',
            'http://%s:%s/blah' % (self.s1.http_interface, self.s1.http_port)
        )
        for s in self.servers:
            self.assertEqual({}, s._keys)
            self.assertEqual(
                ['set-key blah ""', 'del-key blah'],
                s._log
            )


class Failures(TestCase):
    num_nodes = 5
    @inlineCallbacks
    def test_connection_lost_during_prepare(self):
        """ Отключение узла когда он принимает prepare."""
        yield self.wait_when_connection_establied()

        def drop_connection(conn, line):
            if conn.other_side[1] == self.s3.port:
                LockProtocol.send_line_hooks = []
                self.s3.disconnect()

        LockProtocol.send_line_hooks.append((
            re.compile('^paxos-prepare 1$'), drop_connection))

        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah' % (self.s1.http_interface, self.s1.http_port)
        )
        self.assertEqual(OK, result.code)

        result = yield self.wait_when_connection_establied()

        result = yield self.client.request(
            'POST',
            'http://%s:%s/minor' % (self.s1.http_interface, self.s1.http_port)
        )

        add_again = deferLater(reactor, 0.5, lambda:
            self.client.request(
                'POST',
                'http://%s:%s/again' % (self.s1.http_interface, self.s1.http_port)
            )
        )
        result = yield self.s3.when_sync_completed()

        result = yield add_again

        for x, s in enumerate(self.servers):
            try:
                self.assertEqual({'blah': '', 'minor': '', 'again': ''}, s._keys)
            except Exception, e:
                e.args = ('In %s server: ' % (x + 1) + e.args[0],)
                raise

        # full log here
        self.assertEqual(
            [
                'set-key blah ""',
                'set-key minor ""',
                'set-key again ""',
            ],
            self.s2._log
        )
        # last command only, because this node received a snapshot
        self.assertEqual(['set-key again ""'], self.s3._log)


# какие могут быть тесты
# 2. Отключение узла когда он отправляет accepted.
# 3. Подключение узла (чистого) к системе в которой уже есть несколько операций.
# 4. Одновременное добавление лока через разные ноды должно приводить только к одной успешной операции.
# 5. Одновременное добавление разных локов через разные ноды должно приводить только к одинаковым логам на каждом узле.
# 6. После временного отключения и последующего подключения узла, он должен узнавать о состоянии системы
#    и пока не узнает, не должен принимать запросов и участия в процессе выбора.
# 7. Если большой кластер развалился на два, меньший, должен отдавать какой-нибудь подходящий HTTP код ошибки.
