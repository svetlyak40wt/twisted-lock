import random
import time

from ConfigParser import ConfigParser
from twisted.internet.base import DelayedCall
from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol
from twisted.trial import unittest
from ..utils import init_logging

DelayedCall.debug = True

def seed(value):
    def decorator(func):
        func._random_seed = value
        return func
    return decorator


class TestCase(unittest.TestCase):
    def _run(self, method_name, result):
        method = getattr(self, method_name)
        seed = getattr(method, '_random_seed', int(time.time()))
        random.seed(seed)

        def seed_info_adder(failure):
            failure.value.args = (failure.value.args[0] + ' (random seed: %s)' % seed,) + failure.value.args[1:]
            return failure

        d = super(TestCase, self)._run(method_name, result)
        d.addErrback(seed_info_adder)
        return d

class BodyReceiver(Protocol):
    def __init__(self):
        self.done = Deferred()
        self._data = []

    def dataReceived(self, data):
        self._data.append(data)

    def connectionLost(self, reason):
        self.done.callback(''.join(self._data))


def get_body(response):
    """A helper to retrive body of HTTP response."""
    protocol = BodyReceiver()
    response.deliverBody(protocol)
    return protocol.done


logging_config = ConfigParser()
logging_config.add_section('logging')
logging_config.set('logging', 'filename', 'unittest.log')
logging_config.add_section('myself')
logging_config.set('myself', 'listen', '0')

init_logging(logging_config)
