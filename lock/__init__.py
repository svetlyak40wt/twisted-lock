from __future__ import absolute_import
import random

from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor

from . utils import parse_ip, parse_ips


# Special value to specify myself as master
class ME: pass

class LockProtocol(LineReceiver):
    def __init__(self):
        self.other_side = (None, None)


    def connectionMade(self):
        self.sendLine('hello %s %s' % (self.factory.interface, self.factory.port))


    def connectionLost(self, reason):
        print 'Connection to the %s:%s lost.' % self.other_side

        if self.factory.master is None:
            self._add_election_result('error')


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
        if self.factory.connections[addr] is None:
            print 'Adding %s:%s to the connections list' % addr
            self.factory.connections[addr] = self
            self.other_side = addr
            self._check_if_master_should_be_elected()
        else:
            print 'We already have connection with %s:%s' % addr
            self.transport.loseConnection()


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

        self.connections = dict((addr, None) for addr in server_list)
        del self.connections[(self.interface, self.port)]

    def startFactory(self):
        reactor.callLater(10, self._connect_to_others)

    def _connect_to_others(self):
        for (host, port), connection in self.connections.items():
            if connection is None:
                reactor.connectTCP(host, port, self)

    def startedConnecting(self, connector):
        print 'Started to connect to another server: %s:%s' % (
            connector.host,
            connector.port
        )


    def buildProtocol(self, addr):
        print 'Connected to another server: %s:%s' % (addr.host, addr.port)
        return ClientFactory.buildProtocol(self, addr)


    def clientConnectionLost(self, connector, reason):
        print 'Lost connection to %s:%s. Reason: %s' % (
            connector.host,
            connector.port,
            reason
        )

    def clientConnectionFailed(self, connector, reason):
        print 'Connection failed. Reason:', reason



