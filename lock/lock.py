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
from twisted.web import server

from . utils import parse_ip, parse_ips, trace_all, escape
from . exceptions import KeyAlreadyExists, KeyNotFound, PaxosFailed
from . web import Root


def stop_waiting(timeout):
    if not (timeout.called or timeout.cancelled):
        timeout.cancel()


TIMEOUT = 5

# Special value to specify myself as master
class ME: pass


class PaxosProposer(object):
    def __init__(self, factory, number, value):
        self.log = logging.getLogger('paxos.proposer.%s' % factory.port)
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
        stop_waiting(self.prepare_timeout)

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
            self.accept_requests = self.factory.broadcast('paxos-accept %s "%s"' % (self.number, escape(self.value)))
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
        self.log = logging.getLogger('paxos.acceptor.%s' % factory.port)
        self.values = {}

        factory.add_callback('paxos-prepare .*', self.on_prepare)
        factory.add_callback('paxos-accept .*', self.on_accept)

    def on_prepare(self, num, client = None):
        num = int(num)
        if num > self.max_seen_id:
            self.max_seen_id = num
            client.sendLine('paxos-ack %s "%s"' % (num, escape(self.values.get(num, ''))))
        else:
            client.sendLine('paxos-nack %s' % num)

    def on_accept(self, num, value, client = None):
        self.values[int(num)] = value
        client.sendLine('paxos-accepted %s' % num)
        self.factory.on_accept(value)


class LockProtocol(LineReceiver):
    # these hooks are for the functional
    # testing of the protocol
    # this list should contain tuples (regex, callback)
    # if regex matches the received line, then callback will
    # be called with (self, line) arguments.
    send_line_hooks = []

    def __init__(self):
        self.other_side = (None, None)
        self._log = None

    @property
    def log(self):
        if self._log is None:
            self._log = logging.getLogger('lockprotocol.%s' % self.factory.port)
        return self._log


    def connectionMade(self):
        self.sendLine('hello %s %s' % (self.factory.interface, self.factory.port))


    def connectionLost(self, reason):
        self.factory.remove_connection(self)


    def lineReceived(self, line):
        self.log.info('RECV: ' + line)
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
        self.log.info('SEND: ' + line)

        for regex, callback in self.send_line_hooks:
            if regex.match(line) is not None:
                callback(self, line)

        return LineReceiver.sendLine(self, line)


    def cmd_hello(self, host, port, client = None):
        self.log.info('Received hello from %s:%s' % (host, port))

        port = int(port)
        addr = (host, port)
        self.other_side = addr
        self.factory.add_connection(addr, self)



class LockFactory(ClientFactory):
    protocol = LockProtocol

    def __init__(self, config):
        interface, port = parse_ip(config.get('myself', 'listen', '4001'))
        server_list = parse_ips(config.get('cluster', 'nodes', '127.0.0.1:4001'))

        self.port = port
        self.interface = interface
        self.master = None
        self.log = logging.getLogger('lockfactory.%s' % self.port)

        self.connections = {}
        self._all_connections = []
        self.neighbours = [
            conn for conn in server_list
            if conn != (self.interface, self.port)
        ]

        # list of deferreds to be called when
        # connections with all other nodes will be established
        self._connection_waiters = []

        # state
        self._log = []
        self._keys = {}
        self._paxos_id = 0
        self.state = []
        self.callbacks = []

        self.acceptor = PaxosAcceptor(self)

        self._port_listener = reactor.listenTCP(self.port, self, interface = self.interface)
        self._delayed_reconnect = None

        self.web_server = server.Site(Root(self))

        self.http_interface, self.http_port = parse_ip(config.get('web', 'listen', '9001'))
        self._webport_listener = reactor.listenTCP(
            self.http_port,
            self.web_server,
            interface = self.http_interface,
        )


    def close(self):
        self._port_listener.stopListening()
        self._webport_listener.stopListening()
        if self._delayed_reconnect is not None:
            stop_waiting(self._delayed_reconnect)

        for conn in self._all_connections:
            if conn.connected:
                conn.transport.loseConnection()


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

        value = 'set-key %s "%s"' % (key, escape(value))
        return self._start_paxos(value)


    def _start_paxos(self, value):
        """ Start a new paxos iteration.
        """
        self.acceptor.max_seen_id += 1
        proposer = PaxosProposer(self, self.acceptor.max_seen_id, value)
        proposer.deferred.addCallback(self.on_accept)
        return proposer.deferred


    def del_key(self, key):
        if key not in self._keys:
            raise KeyNotFound('Key "%s" not found' % key)

        value = 'del-key %s' % key
        return self._start_paxos(value)


    def add_connection(self, addr, protocol):
        old = self.connections.get(addr, None)

        if old is not None and old.connected and addr > (self.interface, self.port):
            self.log.info('We already have connection with %s:%s (%s)' % (addr[0], addr[1], protocol.transport.getPeer()))
            protocol.transport.loseConnection()
            self._all_connections.remove(protocol)
        else:
            self.log.info('Adding %s:%s to the connections list' % addr)
            self.connections[addr] = protocol
            if len(self.connections) == len(self.neighbours):
                for waiter in self._connection_waiters:
                    waiter.callback(True)
                self._connection_waiters = []


    def remove_connection(self, protocol):
        for key, value in self.connections.items():
            if value == protocol:
                self.log.info(
                    'Connection to the %s:%s (%s) lost.' % (
                        protocol.other_side[0],
                        protocol.other_side[1],
                        protocol.transport.getPeer()
                    )
                )
                del self.connections[key]
                break


    def when_connected(self):
        d = Deferred()
        self._connection_waiters.append(d)
        return d


    def startFactory(self):
        self.log.info('callWhen running %s:%s' % (self.interface, self.port))
        reactor.callWhenRunning(self._reconnect)


    def _reconnect(self):
        self.log.info('reconnecting')
        for host, port in self.neighbours:
            if (host, port) not in self.connections:
                reactor.connectTCP(host, port, self)

        self._delayed_reconnect = reactor.callLater(5, self._reconnect)


    def startedConnecting(self, connector):
        self.log.info('Started to connect to another server: %s:%s' % (
            connector.host,
            connector.port
        ))


    def buildProtocol(self, addr):
        conn = addr.host, addr.port
        self.log.info('Connected to another server: %s:%s' % conn)
        result = ClientFactory.buildProtocol(self, addr)
        self._all_connections.append(result)
        return result


    def clientConnectionFailed(self, connector, reason):
        self.log.info('Connection to %s:%s failed. Reason: %s' % (
            connector.host,
            connector.port,
            reason
        ))


    def broadcast(self, line):
        for connection in self.connections.values():
            connection.sendLine(line)
        return len(self.connections)


    def on_accept(self, value):
        self.master = (self.interface, self.port)
        self._log.append(value)
        splitted = shlex.split(value)
        command = '_log_cmd_' + splitted[0].replace('-', '_')
        cmd = getattr(self, command)
        return cmd(*splitted[1:])


    def _log_cmd_set_key(self, key, value):
        self._keys[key] = value
        return value


    def _log_cmd_del_key(self, key):
        return self._keys.pop(key)


#trace_all(PaxosProposer)
#trace_all(PaxosAcceptor)
#trace_all(LockProtocol)
#trace_all(LockFactory)

