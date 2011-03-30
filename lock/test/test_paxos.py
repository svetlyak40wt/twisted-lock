# -*- coding: utf-8 -*-
from __future__ import absolute_import

import random

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from itertools import chain
from collections import deque
from logbook import LoggerGroup

from ..paxos import Paxos, PrepareTimeout, AcceptTimeout
from ..utils import wait_calls
from . import TestCase as BaseTestCase, seed


class Network(object):
    def __init__(self):
        self.transports = []
        self.log = []
        self._delayed_calls = []

    def broadcast(self, message, from_transport):
        for tr in self.transports:
            tr.send(message, from_transport)

    def learn(self, num, value, client):
        self.log.append((num, value))
        return (num, value)

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
        self.log = []
        self._queue = deque()
        self._delayed_call = None

    def broadcast(self, message):
        self.paxos._logger.info('BROADCASTING "%s"' % message)
        return self.network.broadcast(message, self)

    def send(self, message, from_transport):
        from_transport.paxos._logger.info('SEND "%s" to %s' % (message, self.id))
        self._queue.append((message, from_transport))
        self._reschedule()

    def learn(self, num, value, client):
        self.log.append((num, value))
        return self.network.learn(num, value, client)

    @property
    def quorum_size(self):
        return max(3, len(self.network.transports)/ 2 + 1)

    def _send_next(self):
        if self._queue:
            message, from_transport = self._queue.popleft()

            self.paxos._logger.info('RECV "%s"' % message)
            self.paxos.recv(message, from_transport)
            self._reschedule()

    def _reschedule(self):
        if self._delayed_call is None or self._delayed_call.called:
            delay = abs(random.normalvariate(0, 0.2))
            self._delayed_call = reactor.callLater(delay, self._send_next)
            self.network._delayed_calls.append(self._delayed_call)


class PaxosTests(BaseTestCase):
    def setUp(self):
        self.net = Network()
        self.net.transports = [Transport(i, self.net) for i in xrange(5)]

        def create_paxos(tr, node_id):
            def on_stale(last_accepted_id):
                pass

            def inject_node(rec):
                rec.extra['node'] = node_id

            _logger_group = LoggerGroup(processor=inject_node)
            tr.paxos = Paxos(
                tr,
                on_learn=tr.learn,
                on_stale=on_stale,
                logger_group=_logger_group,
            )

        for tr in self.net.transports:
            create_paxos(tr, tr.id)

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
        yield wait_calls(lambda: self.net.transports[1].log == [(1, 'blah')])
        b = yield self.net.transports[1].paxos.propose('minor')

        yield self.net.wait_delayed_calls()
        self.assertEqual(
            [(1, 'blah')] * 5 + [(2, 'minor')] * 5,
            self.net.log
        )

    @inlineCallbacks
    def test_two_proposes_from_different_nodes_simultaneously(self):
        self.assertEqual([], self.net.log)

        first_round_failed = [False]

        def check_success(result):
            if hasattr(result, 'value') and isinstance(result.value, AcceptTimeout):
                first_round_failed[0] = True

        d1 = self.net.transports[0].paxos.propose('blah')
        d1.addBoth(check_success)

        second_round_failed = [False]

        def _run_second_round():
            def check_fail(result):
                if hasattr(result, 'value') and isinstance(result.value, PrepareTimeout):
                    second_round_failed[0] = True

            d2 = self.net.transports[1].paxos.propose('minor')
            d2.addBoth(check_fail)

        reactor.callLater(0.5, _run_second_round)

        yield self.net.wait_delayed_calls()

        # additional check if check_fail function
        # was really called
        if first_round_failed[0]:
            for tr in self.net.transports:
                self.assertEqual([(2, 'minor')], tr.log, 'Transport id=%s' % tr.id)

        elif second_round_failed[0]:
            for tr in self.net.transports:
                self.assertEqual([(1, 'blah')], tr.log, 'Transport id=%s' % tr.id)

        else:
            for tr in self.net.transports:
                self.assertEqual([(1, 'blah'), (2, 'minor')], tr.log, 'Transport id=%s' % tr.id)

