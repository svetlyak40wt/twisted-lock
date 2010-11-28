# -*- coding: utf-8 -*-
from __future__ import absolute_import

from time import time
from functools import wraps

from twisted.web import resource
from twisted.web.http import CONFLICT, NOT_FOUND
from twisted.web.server import NOT_DONE_YET
from twisted.internet.task import deferLater
from twisted.internet import reactor
from twisted.internet.defer import CancelledError, inlineCallbacks
from twisted.python.log import err

from . exceptions import KeyAlreadyExists, KeyNotFound

def _get_key(path):
    return path[1:]


def long_call(secs):
    def cb():
        return 'Long Call Result'
    return deferLater(reactor, secs, cb)

def delayed(func):
    func = inlineCallbacks(func)

    @wraps(func)
    def wrapper(self, request, *args, **kwargs):
        finished = [False]
        def on_cancel(failure):
            print 'eb3 called: %s' % time()
            err(failure, 'Call to "%s" was interrupted' % request.path)
            finished[0] = True

        request.notifyFinish().addErrback(on_cancel)

        def finish_request(result):
            if finished[0] == False:
                request.finish()

        d = func(self, request, *args, **kwargs)
        d.addBoth(finish_request)
        return NOT_DONE_YET
    return wrapper


class Root(resource.Resource):
    isLeaf = True

    def __init__(self, lock):
        self._lock = lock
        resource.Resource.__init__(self)


    @delayed
    def render_GET(self, request):
        try:
            key = _get_key(request.path)
            value = yield self._lock.get_key(key)
            request.write(value)
        except KeyNotFound:
            request.setResponseCode(NOT_FOUND)


    @delayed
    def render_POST(self, request):
        try:
            key = _get_key(request.path)
            yield self._lock.set_key(key, '')
        except KeyAlreadyExists:
            request.setResponseCode(CONFLICT)


    def render_DELETE(self, request):
        key = _get_key(request.path)
        try:
            return self._lock.del_key(key)
        except KeyNotFound:
            request.setResponseCode(NOT_FOUND)
        return ''


