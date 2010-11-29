from __future__ import absolute_import

import gc

from twisted.trial import unittest
from twisted.web.client import Agent
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
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

        super(Blah, self).__init__(*args, **kwargs)

    def tearDown(self):
        gc.collect()

    @inlineCallbacks
    def test_start_server(self):
        server = LockFactory(cfg(''))
        self.addCleanup(server.close)

        result = yield self.client.request('POST', 'http://127.0.0.1:9001/blah')
        self.assertEqual(EXPECTATION_FAILED, result.code)

