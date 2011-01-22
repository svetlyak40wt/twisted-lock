# -*- coding: utf-8 -*-
from __future__ import absolute_import

import re
import logging
import shlex
import random
import pickle
import base64

from math import ceil
from bisect import bisect_left, bisect_right
from operator import itemgetter
from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.python import failure
from twisted.web import server

from . utils import parse_ip, parse_ips, escape
from . exceptions import KeyAlreadyExists, KeyNotFound, PaxosFailed
from . web import Root


def stop_waiting(timeout):
    if not (timeout.called or timeout.cancelled):
        timeout.cancel()


PREPARE_TIMEOUT = 1
ACCEPT_TIMEOUT = 1
RECONNECT_INTERVAL = 5


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
        self.prepare_timeout = reactor.callLater(PREPARE_TIMEOUT, self.end_prepare)

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
            self.accept_requests = self.factory.broadcast(
                'paxos-accept %s %s "%s"' % (
                    self.number,
                    self.factory.last_accepted_iteration,
                    escape(self.value)
                )
            )
            self.accept_responses = 0
            self.factory.add_callback('paxos-accepted %s' % self.number, self.on_accepted)
            self.accept_timeout = reactor.callLater(ACCEPT_TIMEOUT, self.fail)
        else:
            self.log.error('No accepts was received or they are with some other values')
            self.fail()

    def on_accepted(self, number, client = None):
        self.factory.last_accepted_iteration = int(number)
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
        self.max_seen_id = 0 # maximum seen paxos ID
        self.log = logging.getLogger('paxos.acceptor.%s' % factory.port)
        # sorted collection for storing values of
        # the previous paxos iterations
        self._nums = []   # sorted paxos IDs
        self._values = [] # corresponding values

        factory.add_callback('paxos-prepare .*', self.on_prepare)
        factory.add_callback('paxos-accept .*', self.on_accept)

    def on_prepare(self, num, client = None):
        num = int(num)
        if num > self.max_seen_id:
            self.max_seen_id = num
            client.sendLine('paxos-ack %s "%s"' % (num, escape(self._find_value(num, ''))))
        else:
            client.sendLine('paxos-nack %s' % num)

    def on_accept(self, num, last_accepted_iteration, value, client = None):
        num = int(num)
        last_accepted_iteration = int(last_accepted_iteration)
        if self.factory.last_accepted_iteration != last_accepted_iteration:
            self.factory.set_stale(True)
        else:
            self.factory.set_stale(False)
            self._add_value(int(num), value)
            client.sendLine('paxos-accepted %s' % num)
            self.factory.on_accept(value)
            self.factory.last_accepted_iteration = num

    def _find_value(self, num, default = None):
        """ Searches value for iteration `num` if it was
            received during previous paxos iterations.
        """
        i = bisect_left(self._nums, num)
        if i != len(self._nums) and self._nums[i] == num:
            return self._values[i]
        return default

    def _add_value(self, num, value):
        i = bisect_right(self._nums, num)
        self._nums.insert(i, num)
        self._values.insert(i, value)


class Syncronizer(object):
    """ This class handles replication.
        When the node data become stale, it changes it's
        state and does not participate in Paxos until
        synchronize data with other nodes.
    """
    def __init__(self, factory):
        self._factory = factory
        self._other_node = None

        factory.add_callback('sync-request', self.on_sync_request)
        factory.add_callback('sync-snapshot .*', self.on_sync_snapshot)

    def on_sync_snapshot(self, data, client = None):
        if self._factory.get_stale() is True:
            data = pickle.loads(base64.b64decode(data))
            self._factory._keys = data['keys']
            self._factory.last_accepted_iteration = data['last_accepted_iteration']
            #self._factory.acceptor.max_seen_id = data['max_seen_id']

    def on_sync_request(self, client = None):
        data = dict(
            keys = self._factory._keys,
            last_accepted_iteration = self._factory.last_accepted_iteration,
            #max_seen_id = self._factory.acceptor.max_seen_id,
        )
        client.sendLine(
            'sync-snapshot ' + base64.b64encode(pickle.dumps(data))
        )

    def send_sync_request(self):
        """ Starts sync process. """
        if not self._factory.connections:
            raise RuntimeError('No nodes to sync with.')
        self._other_node = random.choice(self._factory.connections.values())
        self._other_node.sendLine('sync-request')


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
        pass


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


class LockFactory(ClientFactory):
    protocol = LockProtocol

    def __init__(self, config):
        interface, port = parse_ip(config.get('myself', 'listen', '4001'))
        server_list = parse_ips(config.get('cluster', 'nodes', '127.0.0.1:4001'))

        self.port = port
        self.interface = interface
        self.master = None
        self._stale = False
        self._delayed_reconnect = None
        # used to prevent server from reconnecting when it was
        # closed in a unittest
        self._closed = False
        # this flag is used to prevent recursion in _reconnect method
        self._reconnecting = False

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
        # same for sync complete event
        self._sync_completion_waiters = []

        # state
        self._log = []
        self._keys = {}
        # last successful Paxos ID
        self.last_accepted_iteration = 0
        self.state = []
        self.callbacks = []

        self.acceptor = PaxosAcceptor(self)
        self.replicator = Syncronizer(self)

        self._port_listener = reactor.listenTCP(self.port, self, interface = self.interface)

        self.web_server = server.Site(Root(self))

        self.http_interface, self.http_port = parse_ip(config.get('web', 'listen', '9001'))
        self._webport_listener = reactor.listenTCP(
            self.http_port,
            self.web_server,
            interface = self.http_interface,
        )


    def close(self):
        self._closed = True

        d1 = self._port_listener.stopListening()
        d2 = self._webport_listener.stopListening()
        if self._delayed_reconnect is not None:
            stop_waiting(self._delayed_reconnect)

        self.disconnect()


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


    def add_connection(self, conn):
        self.connections[conn.other_side] = conn
        num_disconnected = len(self.neighbours) - len(self.connections)

        if num_disconnected == 0:
            for waiter in self._connection_waiters:
                waiter.callback(True)
            self._connection_waiters = []


    def remove_connection(self, conn):
        for key, value in self.connections.items():
            if value == conn:
                self.log.info(
                    'Connection to the %s:%s (%s) lost.' % (
                        conn.other_side[0],
                        conn.other_side[1],
                        conn.transport.getPeer()
                    )
                )
                del self.connections[key]
                break


    def when_connected(self):
        d = Deferred()
        self._connection_waiters.append(d)
        return d


    def when_sync_completed(self):
        if not self.get_stale():
            raise RuntimeError('Is not syncing')
        d = Deferred()
        self._sync_completion_waiters.append(d)
        return d


    def disconnect(self):
        for conn in self._all_connections:
            if conn.connected:
                conn.transport.loseConnection()


    def startFactory(self):
        self.log.info('callWhen running %s:%s' % (self.interface, self.port))
        reactor.callWhenRunning(self._reconnect)


    def _reconnect(self):
        if not self._closed and not self._reconnecting:
            try:
                self._reconnecting = True

                for host, port in self.neighbours:
                    if (host, port) not in self.connections:
                        self.log.info('reconnecting to %s:%s' % (host, port))
                        reactor.connectTCP(host, port, self)

                self._delayed_reconnect = reactor.callLater(RECONNECT_INTERVAL, self._reconnect)
            finally:
                self._reconnecting = False


    def startedConnecting(self, connector):
        self.log.info('Started to connect to another server: %s:%s' % (
            connector.host,
            connector.port
        ))


    def buildProtocol(self, addr):
        conn = addr.host, addr.port

        result = ClientFactory.buildProtocol(self, addr)
        result.other_side = conn

        self._all_connections.append(result)

        if addr.port in map(itemgetter(1), self.neighbours):
            self.log.info('Connected to another server: %s:%s' % conn)
            self.add_connection(result)
        else:
            self.log.info('Connection from another server accepted: %s:%s' % conn)
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


    def get_stale(self):
        """ Shows if this node is stale and should be synced. """
        return self._stale

    def set_stale(self, value):
        if self._stale is True:
            if value is False:
                self.log.error('Synced, switched to the "normal" mode.')
                # Notify all waiters that node's state was synced.
                for waiter in self._sync_completion_waiters:
                    waiter.callback(True)
                self._sync_completion_waiters = []
        elif value is True:
            self.log.error('Node is out of sync, switched to "sync" mode.')
            self.replicator.send_sync_request()

        self._stale = value


#from . utils import trace_all
#trace_all(PaxosProposer)
#trace_all(PaxosAcceptor)
#trace_all(LockProtocol)
#trace_all(LockFactory)
