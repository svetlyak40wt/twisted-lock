from __future__ import absolute_import

from twisted.trial import unittest
from twisted.web.client import Agent
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, gatherResults
from twisted.web.http import EXPECTATION_FAILED
from StringIO import StringIO

from .. lock import LockFactory
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
    def __init__(self, *args, **kwargs):
        self.cfg1 = cfg('''
[cluster]
nodes = 127.0.0.1:4001, 127.0.0.1:4002, 127.0.0.1:4003
[myself]
listen = 4001
[web]
listen = 9001
        ''')
        self.cfg2 = cfg('''
[cluster]
nodes = 127.0.0.1:4001, 127.0.0.1:4002, 127.0.0.1:4003
[myself]
listen = 4002
[web]
listen = 9002
        ''')
        self.cfg3 = cfg('''
[cluster]
nodes = 127.0.0.1:4001, 127.0.0.1:4002, 127.0.0.1:4003
[myself]
listen = 4003
[web]
listen = 9003
        ''')

        super(Complex, self).__init__(*args, **kwargs)

    def setUp(self):
        self.s1 = LockFactory(self.cfg1)
        self.s2 = LockFactory(self.cfg2)
        self.s3 = LockFactory(self.cfg3)
        self.servers = [self.s1, self.s2, self.s3]

        for s in self.servers:
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

