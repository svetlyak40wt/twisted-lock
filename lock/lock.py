# -*- coding: utf-8 -*-
from __future__ import absolute_import

import re
import logging
import shlex

from math import ceil
from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.python import failure

from . utils import parse_ip, parse_ips, trace_all
from . exceptions import KeyAlreadyExists, KeyNotFound, PaxosFailed

TIMEOUT = 5

# Special value to specify myself as master
class ME: pass


class PaxosProposer(object):
    def __init__(self, factory, number, value):
        self.log = logging.getLogger('paxos.proposer')
        self.deferred = Deferred()
        self.number = number
        self.value = value
        self.factory = factory

        self.requests_count = factory.broadcast('paxos-prepare %s' % self.number)
        self.responses_count = 0

        self.state = 'waiting-promices'
        self.results = []

        factory.add_callback('paxos-ack %s.*' % self.number, self.on_ack)
        factory.add_callback('paxos-nack %s' % self.number, self.on_nack)
        self.prepare_timeout = reactor.callLater(5, self.end_prepare)

    def on_ack(self, number, value, client = None):
        self.results.append(value)
        self.responses_count += 1

        if self.responses_count == self.requests_count:
            self.end_prepare()

    def on_nack(self, number, client = None):
        self.responses_count += 1

        if self.responses_count == self.requests_count:
            self.end_prepare()

    def end_prepare(self):
        self.factory.remove_callback(self.on_ack)
        self.factory.remove_callback(self.on_nack)
        if not self.prepare_timeout.cancelled:
            self.prepare_timeout.cancel()

        num_results = len(self.results)
        threshold = ceil(self.requests_count / 2.0)

        if num_results > threshold:
            self.send_accept()
        else:
            self.log.error('Too small acks received: %s < %s' % (num_results, threshold))
            self.fail()

    def send_accept(self):
        results = filter(None, self.results)

        if len(results) == 0 or self.value in results:
            self.accept_requests = self.factory.broadcast('paxos-accept %s "%s"' % (self.number, self.value))
            self.accept_responses = 0
            self.factory.add_callback('paxos-accepted %s' % self.number, self.on_accepted)
            self.accept_timeout = reactor.callLater(5, self.fail)
        else:
            self.log.error('No accepts was received or they are with some other values')
            self.fail()

    def on_accepted(self, number, client = None):
        self.accept_responses += 1
        threshold = ceil(self.accept_requests / 2.0)
        if self.accept_responses >= threshold:
            if not self.accept_timeout.cancelled:
                self.accept_timeout.cancel()
                self.deferred.callback(self.value)

    def fail(self):
        self.deferred.errback(failure.Failure(
            PaxosFailed('Paxos iteration failed'))
        )


class PaxosAcceptor(object):
    def __init__(self, factory):
        self.factory = factory
        self.max_seen_id = 0
        self.log = logging.getLogger('paxos.acceptor')
        self.values = {}

        factory.add_callback('paxos-prepare .*', self.on_prepare)
        factory.add_callback('paxos-accept .*', self.on_accept)

    def on_prepare(self, num, client = None):
        num = int(num)
        if num > self.max_seen_id:
            self.max_seen_id = num
            client.sendLine('paxos-ack %s "%s"' % (num, self.values.get(num, '')))
        else:
            client.sendLine('paxos-nack %s' % num)

    def on_accept(self, num, value, client = None):
        self.values[int(num)] = value
        client.sendLine('paxos-accepted %s' % num)


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
        parsed = shlex.split(line)
        command = parsed[0]
        args = parsed[1:]
        try:
            cmd = getattr(self, 'cmd_' + command)
        except:
            cmd = self.factory.find_callback(line)
            if cmd is None:
                raise RuntimeError('Unknown command "%s"' % command)

        cmd(client = self, *args)


    def sendLine(self, line):
        print 'SEND:', line
        return LineReceiver.sendLine(self, line)


    def cmd_hello(self, host, port, client = None):
        print 'Received hello from %s:%s' % (host, port)

        port = int(port)
        addr = (host, port)
        self.other_side = addr
        self.factory.add_connection(addr, self)



class LockFactory(ClientFactory):
    protocol = LockProtocol

    def __init__(self, config):
        interface, port = parse_ip(config.get('myself', 'listen'))
        server_list = parse_ips(config.get('cluster', 'nodes'))

        self.port = port
        self.interface = interface
        self.master = None

        self.connections = {}
        self.neighbours = [
            conn for conn in server_list
            if conn != (self.interface, self.port)
        ]

        # state
        self._keys = {}
        self._paxos_id = 0
        self.state = []
        self.callbacks = []

        self.acceptor = PaxosAcceptor(self)

    def add_callback(self, regex, callback):
        self.callbacks.append((re.compile(regex), callback))

    def remove_callback(self, callback):
        self.callbacks = filter(lambda x: x[1] != callback, self.callbacks)

    def find_callback(self, line):
        for regex, callback in self.callbacks:
            if regex.match(line) != None:
                return callback

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

        self._paxos_id += 1
        proposer = PaxosProposer(self, self._paxos_id, 'set-key %s "%s"' % (key, value))

        def cb(result):
            self._keys[key] = value
        proposer.deferred.addCallback(cb)

        return proposer.deferred


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


    def broadcast(self, line):
        for connection in self.connections.values():
            connection.sendLine(line)
        return len(self.connections)

trace_all(PaxosProposer)
#trace_all(PaxosAcceptor)
#trace_all(LockProtocol)
#trace_all(LockFactory)
#
