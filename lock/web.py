from __future__ import absolute_import

from time import time

from twisted.web import resource
from twisted.web.http import CONFLICT, NOT_FOUND
from twisted.web.server import NOT_DONE_YET
from twisted.internet.task import deferLater
from twisted.internet import reactor
from twisted.internet.defer import CancelledError
from twisted.python.log import err

from . exceptions import KeyAlreadyExists, KeyNotFound

def _get_key(path):
    return path[1:]


def long_call(secs):
    def cb():
        return 'Long Call Result'
    return deferLater(reactor, secs, cb)

class Root(resource.Resource):
    isLeaf = True

    def __init__(self, lock):
        self._lock = lock
        resource.Resource.__init__(self)


    def render_GET(self, request):
        def cb1(result):
            print 'cb1 called: %s' % time()
            a = 10/0
            request.write(result)
            request.finish()

        def eb1(failure):
            err(failure, 'Handled in eb1')
            print 'eb1 called: %s' % time()
            if failure.type == CancelledError:
                print 'Cancelled'
            else:
                request.write('ERROR 1\n')
                request.finish()

        def eb3(failure, d):
            print 'eb3 called: %s' % time()
            d.cancel()
            err(failure, 'Test was interrupted')

        secs = int(request.args.get('sec', [1])[0])

        d = long_call(secs)
        d.addCallback(cb1)
        d.addErrback(eb1)
        request.notifyFinish().addErrback(eb3, d)
        return NOT_DONE_YET

        key = _get_key(request.path)
        try:
            return self._lock.get_key(key)
        except KeyNotFound:
            request.setResponseCode(NOT_FOUND)
        return NOT_DONE_YET


    def render_POST(self, request):
        key = _get_key(request.path)
        try:
            self._lock.set_key(key, '')
        except KeyAlreadyExists:
            request.setResponseCode(CONFLICT)
        return ''


    def render_DELETE(self, request):
        key = _get_key(request.path)
        try:
            return self._lock.del_key(key)
        except KeyNotFound:
            request.setResponseCode(NOT_FOUND)
        return ''


