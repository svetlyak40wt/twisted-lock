#!env/bin/python
import sys

from twisted.internet import reactor
from twisted.web import server
from lock import LockFactory
from lock.web import Root
from lock.utils import parse_ip

from ConfigParser import SafeConfigParser

def main():
    config = SafeConfigParser()
    config.read(sys.argv[1])

    lock = LockFactory(config)
    web = server.Site(Root(lock))

    reactor.listenTCP(lock.port, lock, interface = lock.interface)

    interface, port = parse_ip(config.get('web', 'listen'))
    reactor.listenTCP(port, web, interface = interface)

    reactor.run()


if __name__ == '__main__':
    main()


