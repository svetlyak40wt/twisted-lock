#!env/bin/python
import sys

from twisted.internet.protocol import ClientFactory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor


class LockProtocol(LineReceiver):
    def connectionMade(self):
        self.sendLine('hello %s %s' % self.factory.address)


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
        LineReceiver.sendLine(self, line)


    def cmd_hello(self, host, port):
        print 'Received hello from %s:%s' % (host, port)

        port = int(port)
        if self.factory.connections[(host, port)] is None:
            print 'Adding %s:%s to the connections list' % (host, port)
            self.factory.connections[(host, port)] = self
        else:
            print 'We already have connection with %s:%s' % (host, port)
            self.transport.loseConnection()



class LockFactory(ClientFactory):
    protocol = LockProtocol

    def __init__(self, port):
        self.port = port
        self.address = ('localhost', port)
        self.master = None

        self.connections = dict((('localhost', 2000 + x), None) for x in xrange(2))
        del self.connections[('localhost', self.port)]

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



def main(port):
    # Next lines are magic:
    factory = LockFactory(port)
    #factory.protocol = LockProtocol

    # 8007 is the port you want to run under. Choose something >1024
    reactor.listenTCP(port, factory)
    reactor.run()


if __name__ == '__main__':
    port = int(sys.argv[1])
    main(port)



