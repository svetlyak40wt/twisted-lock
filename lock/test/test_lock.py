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


    @inlineCallbacks
    def test_start_server(self):
        server = LockFactory(cfg(''))
        self.addCleanup(server.close)

        result = yield self.client.request('POST', 'http://127.0.0.1:9001/blah')
        self.assertEqual(EXPECTATION_FAILED, result.code)


    @inlineCallbacks
    def test_node_become_a_master(self):
        server1 = LockFactory(self.cfg1)
        server2 = LockFactory(self.cfg2)
        server3 = LockFactory(self.cfg3)

        self.addCleanup(server1.close)
        self.addCleanup(server2.close)
        self.addCleanup(server3.close)

        yield gatherResults([
            server1.when_connected(),
            server2.when_connected(),
            server3.when_connected(),
        ])
        self.assertEqual(None, server1.master)
        result = yield self.client.request('POST', 'http://127.0.0.1:9001/blah')
        self.assertEqual((server1.interface, server1.port), server1.master)

