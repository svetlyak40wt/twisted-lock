# -*- coding: utf-8 -*-
from __future__ import absolute_import

import random
import time

from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor

from ..paxos import Paxos, PrepareTimeout
from ..utils import wait_calls
from itertools import chain


class Network(object):
    def __init__(self):
        self.transports = []
        self.log = []
        self._delayed_calls = []

    def broadcast(self, message, from_transport):
        for tr in self.transports:
            tr.send(message, from_transport)

    def learn(self, num, value):
        self.log.append((num, value))

    def wait_delayed_calls(self):
        return wait_calls(
            lambda: all(
                (c is None or c.cancelled or c.called)
                for c in chain(
                    self._delayed_calls,
                    *(tr.paxos._get_timeouts() for tr in self.transports)
                )
            )
        )


class Transport(object):
    def __init__(self, id, network):
        self.id = id
        self.network = network
        self.paxos = None

    def broadcast(self, message):
        print '%s broadcasting "%s"' % (self.id, message)
        return self.network.broadcast(message, self)

    def learn(self, num, value):
        self.network.learn(num, value)

    def send(self, message, from_transport):
        print '%s sending "%s" to %s' % (from_transport.id, message, self.id)
        self.network._delayed_calls.append(
            reactor.callLater(random.random(), self.paxos.recv, message, from_transport)
        )

    @property
    def quorum_size(self):
        return max(3, len(self.network.transports)/ 2 + 1)


class PaxosTests(unittest.TestCase):
    def setUp(self):
        self.seed = int(time.time())
        random.seed(self.seed)
        print 'Random seed: %s' % self.seed

        self.net = Network()
        self.net.transports = [Transport(i, self.net) for i in xrange(5)]
        for tr in self.net.transports:
            tr.paxos = Paxos(tr)

    def tearDown(self):
        for tr in self.net.transports:
            tr.paxos._cancel_timeouts()

    @inlineCallbacks
    def test_basic(self):
        self.assertEqual([], self.net.log)

        result = yield self.net.transports[0].paxos.propose('blah')
        yield self.net.wait_delayed_calls()

        self.assertEqual((1, 'blah'), result)
        self.assertEqual([(1, 'blah')] * 5, self.net.log)

    @inlineCallbacks
    def test_two_proposes(self):
        self.assertEqual([], self.net.log)
        self.net.transports[0].paxos.propose('blah')
        self.net.transports[0].paxos.propose('minor')

        yield self.net.wait_delayed_calls()
        self.assertEqual(
            [(1, 'blah')] * 5 + [(2, 'minor')] * 5,
            self.net.log
        )

    @inlineCallbacks
    def test_two_proposes_from_different_nodes_in_sequence(self):
        self.assertEqual([], self.net.log)
        a = yield self.net.transports[0].paxos.propose('blah')

        # Waiting when paxos on node 1 will learn the new value
        yield wait_calls(lambda: self.net.transports[1].paxos.id == 1)
        b = yield self.net.transports[1].paxos.propose('minor')

        yield self.net.wait_delayed_calls()
        print 'RESULT:', a, b
        self.assertEqual(
            [(1, 'blah')] * 5 + [(2, 'minor')] * 5,
            self.net.log
        )

    @inlineCallbacks
    def test_two_proposes_from_different_nodes_simultaneously(self):
        self.assertEqual([], self.net.log)

        def check_success(result):
            self.assertEqual((1, 'blah'), result)

        d1 = self.net.transports[0].paxos.propose('blah')
        d1.addBoth(check_success)

        second_round_failed = [False]

        def _run_second_round():
            def check_fail(result):
                if result.value == PrepareTimeout:
                    second_round_failed[0] = True

            d2 = self.net.transports[1].paxos.propose('minor')
            d2.addBoth(check_fail)

        reactor.callLater(0.5, _run_second_round)

        yield self.net.wait_delayed_calls()

        # additional check if check_fail function
        # was really called
        self.assertEqual(True, second_round_failed[0])
        self.assertEqual(
            [(1, 'blah')] * 5,
            self.net.log
        )

