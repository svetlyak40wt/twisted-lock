from __future__ import absolute_import

import re

from twisted.trial import unittest
from twisted.web.client import Agent
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, gatherResults
from twisted.web.http import EXPECTATION_FAILED, OK
from StringIO import StringIO

from .. lock import LockFactory, LockProtocol
from .. config import Config
from .. utils import init_logging

def cfg(text):
    config = Config()
    config.readfp(StringIO(text))
    return config


class TestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        init_logging()
        self.client = Agent(reactor)
        super(TestCase, self).__init__(*args, **kwargs)


class Simple(TestCase):
    @inlineCallbacks
    def test_start_server(self):
        server = LockFactory(cfg(''))
        self.addCleanup(server.close)

        result = yield self.client.request('POST', 'http://127.0.0.1:9001/blah')
        self.assertEqual(EXPECTATION_FAILED, result.code)


class Complex(TestCase):
    num_nodes = 3

    def __init__(self, *args, **kwargs):
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

        super(Complex, self).__init__(*args, **kwargs)

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


    @inlineCallbacks
    def test_connection_lost_during_prepare(self):
        yield self.wait_when_connection_establied()

        def drop_connection(conn, line):
            if conn.other_side[1] == self.s3.port:
                LockProtocol.send_line_hooks = []
                conn.transport.loseConnection()

        LockProtocol.send_line_hooks.append((
            re.compile('^paxos-prepare 1$'), drop_connection))

        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah' % (self.s1.http_interface, self.s1.http_port)
        )
        self.assertEqual(EXPECTATION_FAILED, result.code)

        # retry
        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah' % (self.s1.http_interface, self.s1.http_port)
        )
        self.assertEqual(OK, result.code)

        for s in self.servers:
            self.assertEqual({'blah': ''}, s._keys)


