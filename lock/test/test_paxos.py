# -*- coding: utf-8 -*-
from __future__ import absolute_import

from twisted.trial import unittest
from ..paxos import Paxos


class Network(object):
    def __init__(self):
        self.transports = []
        self.log = []

    def broadcast(self, message, from_transport):
        for tr in self.transports:
            tr.send(message, from_transport)

    def learn(self, num, value):
        self.log.append((num, value))


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
        self.paxos.recv(message, from_transport)

    @property
    def quorum_size(self):
        return max(3, len(self.network.transports)/ 2 + 1)


class PaxosTests(unittest.TestCase):
    def setUp(self):
        self.net = Network()
        self.net.transports = [Transport(i, self.net) for i in xrange(5)]
        for tr in self.net.transports:
            tr.paxos = Paxos(tr)

    def test_basic(self):
        self.assertEqual([], self.net.log)
        self.net.transports[0].paxos.propose('blah')
        self.assertEqual([(1, 'blah')] * 5, self.net.log)

    def test_two_proposes(self):
        self.assertEqual([], self.net.log)
        self.net.transports[0].paxos.propose('blah')
        self.net.transports[0].paxos.propose('minor')
        self.assertEqual(
            [(1, 'blah')] * 5 + [(2, 'minor')] * 5,
            self.net.log
        )

    def test_two_proposes_from_different_nodes(self):
        self.assertEqual([], self.net.log)
        self.net.transports[0].paxos.propose('blah')
        self.net.transports[1].paxos.propose('minor')
        self.assertEqual(
            [(1, 'blah')] * 5 + [(2, 'minor')] * 5,
            self.net.log
        )

