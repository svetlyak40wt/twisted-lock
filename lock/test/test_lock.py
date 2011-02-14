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
from ConfigParser import ConfigParser

from .. lock import LockFactory, LockProtocol
from .. config import Config
from .. utils import init_logging

DelayedCall.debug = True
logging_config = ConfigParser()
logging_config.add_section('logging')
logging_config.set('logging', 'filename', 'unittest.log')

def cfg(text):
    config = Config()
    config.readfp(StringIO(text))
    return config


class TestCase(unittest.TestCase):
    num_nodes = 3

    def __init__(self, *args, **kwargs):
        init_logging(logging_config)
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
        self.addCleanup(self._close_servers)

    def _close_servers(self):
        for server in self.servers:
            if server is not None:
                server.close()

    def stop_server(self, number):
        s = getattr(self, 's%s' % number)
        setattr(self, 's%s' % number, None)
        s.close()
        self.servers[number - 1] = None

    def start_server(self, number):
        s = LockFactory(self.configs[number - 1])
        setattr(self, 's%s' % number, s)
        self.servers[number - 1] = s

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


class Disconnections(TestCase):
    num_nodes = 5

    @inlineCallbacks
    def _run_disconnection_test(self):
        """ Adds three keys into the cluster. """
        yield self.wait_when_connection_establied()

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

        self._assert_servers_consistency('blah', 'minor', 'again')

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

    @inlineCallbacks
    def test_connection_lost_during_accept(self):
        """ Отключение узла когда он принимает accept."""
        def drop_connection(conn, line):
            if conn.other_side[1] == self.s3.port:
                LockProtocol.send_line_hooks = []
                self.s3.disconnect()

        LockProtocol.send_line_hooks = [(
            re.compile(r'^paxos-accept 1 .*'), drop_connection)]
        yield self._run_disconnection_test()

    @inlineCallbacks
    def test_connection_lost_during_prepare(self):
        """ Отключение узла когда он принимает prepare."""
        def drop_connection(conn, line):
            if conn.other_side[1] == self.s3.port:
                LockProtocol.send_line_hooks = []
                self.s3.disconnect()

        LockProtocol.send_line_hooks = [(
            re.compile('^paxos-prepare 1$'), drop_connection)]

        yield self._run_disconnection_test()

    @inlineCallbacks
    def test_connect_node_to_working_system(self):
        """ Подключение узла (чистого) к системе в которой уже есть несколько операций. """
        result = yield self.stop_server(3)

        yield self._add_key('blah')
        yield self._add_key('minor')
        yield self._add_key('again')

        self.start_server(3)

        yield self.wait_when_connection_establied()
        yield self._add_key('and-again')
        yield self._add_key('and-one-more')

        self._assert_servers_consistency('blah', 'minor', 'again', 'and-again', 'and-one-more')
        self.assertEqual(False, self.s3.get_stale())
        self.assertEqual(['set-key and-one-more ""'], self.s3._log)


    @inlineCallbacks
    def _add_key(self, key):
        result = yield self.client.request(
            'POST',
            'http://%s:%s/%s' % (self.s1.http_interface, self.s1.http_port, key)
        )
        self.assertEqual(OK, result.code)

    def _assert_servers_consistency(self, *args):
        for x, s in enumerate(self.servers):
            if s is not None:
                try:
                    self.assertEqual(
                        dict((item, '') for item in args),
                        s._keys
                    )
                except Exception, e:
                    e.args = ('In %s server: ' % (x + 1) + e.args[0],)
                    raise



# какие могут быть тесты
# 4. Одновременное добавление лока через разные ноды должно приводить только к одной успешной операции.
# 5. Одновременное добавление разных локов через разные ноды должно приводить только к одинаковым логам на каждом узле.
# 6. После временного отключения и последующего подключения узла, он должен узнавать о состоянии системы
#    и пока не узнает, не должен принимать запросов и участия в процессе выбора.
# 7. Если большой кластер развалился на два, меньший, должен отдавать какой-нибудь подходящий HTTP код ошибки.
