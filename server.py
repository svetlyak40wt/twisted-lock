#!env/bin/python
import sys

from twisted.internet import reactor
from lock import LockFactory

from ConfigParser import SafeConfigParser

def main():
    config = SafeConfigParser()
    config.read(sys.argv[1])

    factory = LockFactory(config)
    #factory.protocol = LockProtocol

    # 8007 is the port you want to run under. Choose something >1024
    reactor.listenTCP(factory.port, factory)
    reactor.run()


if __name__ == '__main__':
    main()


