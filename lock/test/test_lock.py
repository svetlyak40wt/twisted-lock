from __future__ import absolute_import

import gc

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

class Blah(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.client = Agent(reactor)
        init_logging()

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

        super(Blah, self).__init__(*args, **kwargs)

    def setUp(self):
        selfserver1 = LockFactory(self.cfg1)
        selfserver2 = LockFactory(self.cfg2)
        selfserver3 = LockFactory(self.cfg3)

        self.addCleanup(self.server1.close)
        self.addCleanup(self.server2.close)
        self.addCleanup(self.server3.close)


    @inlineCallbacks
    def wait_when_connection_establied(self):
        yield gatherResults([
            self.server1.when_connected(),
            self.server2.when_connected(),
            self.server3.when_connected(),
        ])


    @inlineCallbacks
    def test_start_server(self):
        server = LockFactory(cfg(''))
        self.addCleanup(server.close)

        result = yield self.client.request('POST', 'http://127.0.0.1:9001/blah')
        self.assertEqual(EXPECTATION_FAILED, result.code)


    @inlineCallbacks
    def test_node_become_a_master(self):
        yield self.wait_when_connection_establied()
        server = self.server1
        self.assertEqual(None, server.master)
        result = yield self.client.request(
            'POST',
            'http://%s:%s/blah' % (server.http_interface, server.http_port)
        )
        self.assertEqual((server.interface, server.port), server.master)

