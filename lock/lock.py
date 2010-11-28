# -*- coding: utf-8 -*-
from __future__ import absolute_import
import random

from math import ceil
from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.internet.defer import Deferred

from . utils import parse_ip, parse_ips
from . exceptions import KeyAlreadyExists, KeyNotFound

TIMEOUT = 5

# Special value to specify myself as master
class ME: pass


class PaxosProposer(object):
    def __init__(self, factory, number, value):
        self.deferred = Deferred()
        self.number = number
        self.value = value

        self.requests_count = factory.broadcast('paxos-prepare %s' % self.number)
        self.responses_count = 0

        self.state = 'waiting-promices'
        self.results = []

        self.factory.add_callback('paxos-ack %s.*' % self.number, self.on_ack)
        self.factory.add_callback('paxos-nack %s.*' % self.number, self.on_nack)
        self.timeout = reactor.callLater(5, self.end_prepare)

    def on_ack(self, line):
        value = line.replace('paxos-ack %s' % self.number, '').strip()
        self.results.append(value)
        self.responses_count += 1

        if self.responses_count == self.requests_count:
            self.end_prepare()

    def on_nack(self, line):
        self.responses_count += 1

        if self.responses_count == self.requests_count:
            self.end_prepare()

    def end_prepare(self):
        self.factory.remove_callback(self.on_ack)
        self.factory.remove_callback(self.on_nack)
        self.timeout.cancel()

        if len(self.results) > ceil(self.requests_count / 2.0):
            self.send_accept()


    def send_accept(self):
        if self.value in self.results:
            # TODO продолжить тут
            value = random



class LockProtocol(LineReceiver):
    def __init__(self):
        self.other_side = (None, None)


    def connectionMade(self):
        self.sendLine('hello %s %s' % (self.factory.interface, self.factory.port))


    def connectionLost(self, reason):
        print 'Connection to the %s:%s lost.' % self.other_side
        self.factory.remove_connection(self.other_side)



    def lineReceived(self, line):
        print 'RECV:', line
        line = line.split()
        command = line[0]
        args = line[1:]
        try:
            cmd = getattr(self, 'cmd_' + command)
        except:
            raise RuntimeError('Unknown command "%s"' % command)

        cmd(*args)


    def sendLine(self, line):
        print 'SEND:', line
        return LineReceiver.sendLine(self, line)


    def cmd_hello(self, host, port):
        print 'Received hello from %s:%s' % (host, port)

        port = int(port)
        addr = (host, port)
        self.other_side = addr
        self.factory.add_connection(addr, self)
        self._check_if_master_should_be_elected()


    def _check_if_master_should_be_elected(self):
        if self.factory.master is None and \
                all(self.factory.connections.values()):
            self._start_master_election()


    def _start_master_election(self):
        self.factory.weight = random.randint(0, 100)
        self.factory.election_results = {}

        for addr, connection in self.factory.connections.items():
            if connection is not None:
                self.factory.election_results[addr] = None
                connection.sendLine('master_election %d' % self.factory.weight)


    def _add_election_result(self, result):
        self.factory.election_results[self.other_side] = result
        done = len(filter(lambda x: x is None, self.factory.election_results.values())) == 0
        if done:
            approved = len(filter(lambda x: x == 'approved', self.factory.election_results.values()))
            if approved > len(self.factory.election_results) / 2:
                self._set_master(ME)


    def _set_master(self, master):
        self.factory.master = master

        if master == ME:
            print 'Horay! I am the MASTER!'
            for connection in self.factory.connections.values():
                connection.sendLine('i_am_master')
        else:
            print 'New master was elected: %s:%s' % master.other_side

        self.factory.election_results = None


    def cmd_master_election(self, weight):
        if self.factory.weight is None:
            self._start_master_election()

        if int(weight) > self.factory.weight:
            #import pdb;pdb.set_trace()
            result = self.sendLine('master_approve')
        else:
            #kimport pdb;pdb.set_trace()
            result = self.sendLine('master_decline')


    def cmd_master_approve(self):
        self._add_election_result('approved')


    def cmd_master_decline(self):
        self._add_election_result('declined')


    def cmd_i_am_master(self):
        self._set_master(self)



class LockFactory(ClientFactory):
    protocol = LockProtocol

    def __init__(self, config):
        interface, port = parse_ip(config.get('myself', 'listen'))
        server_list = parse_ips(config.get('cluster', 'nodes'))

        self.port = port
        self.interface = interface
        self.master = None
        self.weight = None

        self.connections = {}
        self.neighbours = [
            conn for conn in server_list
            if conn != (self.interface, self.port)
        ]

        # state
        self._keys = {}


    def get_key(self, key):
        d = Deferred()
        def cb():
            if key not in self._keys:
                raise KeyNotFound('Key "%s" not found' % key)
            return self._keys[key]
        d.addCallback(cb)
        return d


    def set_key(self, key, value):
        if key in self._keys:
            raise KeyAlreadyExists('Key "%s" already exists' % key)

        self._keys[key] = value


    def del_key(self, key):
        if key not in self._keys:
            raise KeyNotFound('Key "%s" not found' % key)
        return self._keys.pop(key)


    def add_connection(self, addr, protocol):
        old = self.connections.get(addr, None)

        if old is not None:
            print 'We already have connection with %s:%s' % addr
            protocol.transport.loseConnection()
        else:
            print 'Adding %s:%s to the connections list' % addr
            self.connections[addr] = protocol


    def remove_connection(self, addr):
        del self.connections[addr]


    def startFactory(self):
        reactor.callWhenRunning(self._reconnect)


    def _reconnect(self):
        for host, port in self.neighbours:
            if (host, port) not in self.connections:
                reactor.connectTCP(host, port, self)

        reactor.callLater(5, self._reconnect)


    def startedConnecting(self, connector):
        print 'Started to connect to another server: %s:%s' % (
            connector.host,
            connector.port
        )


    def buildProtocol(self, addr):
        conn = addr.host, addr.port
        print 'Connected to another server: %s:%s' % conn
        return ClientFactory.buildProtocol(self, addr)


    def clientConnectionFailed(self, connector, reason):
        print 'Connection to %s:%s failed. Reason: %s' % (
            connector.host,
            connector.port,
            reason
        )




