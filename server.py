#!env/bin/python
# -*- coding: utf-8 -*-
import sys

from twisted.internet import reactor
from lock.lock import LockFactory
from lock.utils import init_logging
from lock.config import Config


def main():
    config = Config(sys.argv[1])

    init_logging(config)

    lock = LockFactory(config)
    reactor.run()
    lock.close()


if __name__ == '__main__':
    main()

