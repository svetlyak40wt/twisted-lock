# -*- coding: utf-8 -*-
from __future__ import absolute_import

import re
import logging
import shlex
import random
import pickle
import base64

from operator import itemgetter
from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.web import server

from .utils import parse_ip, parse_ips, escape
from .exceptions import KeyAlreadyExists, KeyNotFound
from .web import Root
from .paxos import Paxos


PREPARE_TIMEOUT = 5
ACCEPT_TIMEOUT = 5
RECONNECT_INTERVAL = 5


class Syncronizer(object):
    """ This class handles replication.
        When the node data become stale, it changes it's
        state and does not participate in Paxos until
        synchronize data with other nodes.
    """
    def __init__(self, factory):
        self._factory = factory
        # a list of change subscribers
        self._subscribers = set()
        self._syncing_with_node = None

        factory.add_callback('^sync_subscribe$', self.on_sync_subscribe)
        factory.add_callback('^sync_unsubscribe$', self.on_sync_subscribe)
        factory.add_callback('^sync_snapshot .*$', self.on_sync_snapshot)

    def on_sync_snapshot(self, line, client = None):
        if self._syncing_with_node is not None:
            cmd_name, data = line.split(' ', 1)

            if self._factory.get_stale() is True:
                data = pickle.loads(base64.b64decode(data))
                self._factory._keys = data['keys']
                self._factory._epoch = data['epoch']
                self._factory.paxos.set_state(data['paxos'])

    def on_sync_subscribe(self, line, client = None):
        self._subscribers.add(client)
        data = dict(
            keys=self._factory._keys,
            epoch=self._factory._epoch,
            paxos=self._factory.paxos.get_state(),
        )
        client.sendLine(
            'sync_snapshot ' + base64.b64encode(pickle.dumps(data))
        )

    def on_sync_unsubscribe(self, line, client = None):
        if client in self._subscribers:
            self._subscribers.remove(client)

    def subscribe(self):
        """ Starts sync process. """
        if not self._factory.connections:
            raise RuntimeError('No nodes to sync with.')

        available_nodes = [
            client
            for client in self._factory.connections.values()
            if client.other_side != (self._factory.interface, self._factory.port)
        ]
        self._syncing_with_node = random.choice(available_nodes)
        self._syncing_with_node.sendLine('sync_subscribe')

    def unsubscribe(self):
        """ Starts sync process. """
        if self._syncing_with_node is not None:
            self._syncing_with_node.sendLine('sync_unsubscribe')
            self._syncing_with_node = None


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

        cmd = self.factory.find_callback(line)
        if cmd is None:
            raise RuntimeError('Unknown command "%s"' % line)
        else:
            cmd(line, client=self)

        #parsed = shlex.split(line)
        #command = parsed[0]
        #args = parsed[1:]
        #try:
        #    cmd = getattr(self, 'cmd_' + command)
        #except:
        #    cmd = self.factory.find_callback(line)
        #    if cmd is None:
        #        raise RuntimeError('Unknown command "%s"' % command)

        #cmd(client = self, *args)

    def send(self, line, transport):
        self.sendLine(line)

    def sendLine(self, line):
        self.log.info('SEND: ' + line)

        for regex, callback in self.send_line_hooks:
            if regex.match(line) is not None:
                callback(self, line)

        return LineReceiver.sendLine(self, line)


class LockFactory(ClientFactory):
    protocol = LockProtocol

    def __init__(self, config):
        self.paxos = Paxos(self, self.learn)

        interface, port = parse_ip(config.get('myself', 'listen', '4001'))
        self.neighbours = parse_ips(config.get('cluster', 'nodes', '127.0.0.1:4001'))
        self._first_connect_delay = float(config.get('cluster', 'first_connect_delay', 0))

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

        # list of deferreds to be called when
        # connections with all other nodes will be established
        self._connection_waiters = []
        # same for sync complete event
        self._sync_completion_waiters = []

        # state
        self._log = []
        self._epoch = 0
        self._keys = {}
        # last successful Paxos ID
        self.last_accepted_iteration = 0
        self.state = []
        self.callbacks = []
        # map paxos-id -> proposer
        self._proposers = {}

        self.add_callback('^paxos_learn.*$', self.check_is_this_node_is_stale)
        self.add_callback('^paxos_.*$', self.paxos.recv)

        self.replicator = Syncronizer(self)

        self.log.debug('Opening the port %s:%s' % (self.interface, self.port))
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

    def get_status(self):
        """Returns a list of tuples (param_name, value)."""
        return [
            ('bind', '%s:%s' % (self.interface, self.port)),
            ('master', self.master),
            ('stale', self._stale),
            ('max_seen_id', self.acceptor.max_seen_id),
            ('last_accepted_id', self.last_accepted_iteration),
            ('num_connections', len(self.connections)),
        ]

    def get_key(self, key):
        d = Deferred()
        def cb():
            if key not in self._keys:
                raise KeyNotFound('Key "%s" not found' % key)
            return self._keys[key]
        d.addCallback(cb)
        return d

    def set_key(self, key, value):
        value = 'set-key %s "%s"' % (key, escape(value))
        return self.paxos.propose(value)

    def del_key(self, key):
        value = 'del-key %s' % key
        return self.paxos.propose(value)

    def add_connection(self, conn):
        self.connections[conn.other_side] = conn
        self._notify_waiters_if_needed()

    def _notify_waiters_if_needed(self):
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
        self._notify_waiters_if_needed()
        return d

    def when_sync_completed(self):
        d = Deferred()
        if self.get_stale():
            # sync in progress
            self._sync_completion_waiters.append(d)
        else:
            # we are not a stale node
            d.callback(True)
        return d

    def disconnect(self):
        for conn in self._all_connections:
            if conn.connected:
                conn.transport.loseConnection()

    def startFactory(self):
        self.log.info('factory started at %s:%s' % (self.interface, self.port))
        if self._first_connect_delay > 0:
            def delay_connect():
                # delay connection to other server
                # this is needed to start few test servers
                # on the same machine without errors
                self._delayed_reconnect = reactor.callLater(self._first_connect_delay, self._reconnect)

            reactor.callWhenRunning(delay_connect)
        else:
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

    # BEGIN Paxos Transport methods
    def broadcast(self, line):
        if line.startswith('paxos_learn'):
            # Add info about current epoch
            line += ' %s' % self._epoch

        for connection in self.connections.values():
            connection.sendLine(line)
        return len(self.connections)

    @property
    def quorum_size(self):
        return max(2, len(self.connections)/ 2 + 1)

    def learn(self, num, value):
        """First callback in the paxos result accepting chain."""
        logging.info('factory.learn %s' % (value,))

        self.master = (self.interface, self.port)
        self._log.append(value)
        self._epoch += 1

        splitted = shlex.split(value)
        command_name, args = splitted[0], splitted[1:]

        command = '_log_cmd_' + command_name.replace('-', '_')
        cmd = getattr(self, command)

        try:
            return cmd(*args)
        except Exception, e:
            logging.exception('command "%s" failed' % command_name)
            raise

    # END Paxos Transport methods

    # START Sync related stuff
    def check_is_this_node_is_stale(self, line, client):
        """This method process 'paxos_learn' command before the Paxos class.

        It strips the Epoch number from it and checks if this replica is stale.
        """
        line, epoch = line.rsplit(' ', 1)
        epoch = int(epoch)
        if epoch > self._epoch:
            self.set_stale(True)
        else:
            self.set_stale(False)
            self.paxos.recv(line, client)

    def _log_cmd_set_key(self, key, value):
        if key in self._keys:
            raise KeyAlreadyExists('Key "%s" already exists' % key)

        self._keys[key] = value
        return value

    def _log_cmd_del_key(self, key):
        if key not in self._keys:
            raise KeyNotFound('Key "%s" not found' % key)

        return self._keys.pop(key)

    def get_stale(self):
        """ Shows if this node is stale and should be synced. """
        return self._stale

    def set_stale(self, value):
        if self._stale is True:
            if value is False:
                self.log.error('Synced, switched to the "normal" mode.')
                self.replicator.unsubscribe()

                # Notify all waiters that node's state was synced.
                for waiter in self._sync_completion_waiters:
                    waiter.callback(True)
                self._sync_completion_waiters = []
        elif value is True:
            self.log.error('Node is out of sync, switched to "sync" mode.')
            self.replicator.subscribe()

        self._stale = value


def stop_waiting(timeout):
    if not (timeout.called or timeout.cancelled):
        timeout.cancel()

#from . utils import trace_all
#trace_all(LockProtocol)
#trace_all(LockFactory)
