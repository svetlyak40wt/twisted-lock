#!env/bin/python
# -*- coding: utf-8 -*-
import sys

from twisted.internet import reactor
from lock.lock import LockFactory
from lock.utils import init_logging
from lock.utils import Config


def main():
    config = Config()
    config.read(sys.argv[1])

    init_logging()

    lock = LockFactory(config)
    reactor.run()
    lock.close()


if __name__ == '__main__':
    main()

