# -*- coding: utf-8 -*-
from __future__ import absolute_import

from functools import wraps

from logbook import Logger
from twisted.web import resource
from twisted.web.http import CONFLICT, NOT_FOUND, INTERNAL_SERVER_ERROR, EXPECTATION_FAILED
from twisted.web.server import NOT_DONE_YET
from twisted.internet.task import deferLater
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.python.log import err
from twisted.python.failure import Failure

from . exceptions import KeyAlreadyExists, KeyNotFound
from .paxos import PaxosError


def _get_key_from_path(path):
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
        log = Logger('web')

        def on_cancel(failure):
            err(failure, 'Call to "%s" was interrupted' % request.path)
            finished[0] = True

        request.notifyFinish().addErrback(on_cancel)

        def finish_request(result):
            log.debug('%s(%r, args=%r, kwargs=%r)=%s' % (func.__name__, request, args, kwargs, result))

            if isinstance(result, Failure):
                request.setResponseCode(INTERNAL_SERVER_ERROR)
                log.exception('Call to %s(%r, args=%r, kwargs=%r) failed' % (func.__name__, request, args, kwargs), exc_info = (result.type, result.value, result.getTracebackObject()))

            if finished[0] == False:
                request.finish()

        log.debug('Calling %s(%r, args=%r, kwargs=%r)' % (func.__name__, request, args, kwargs))
        d = func(self, request, *args, **kwargs)
        log.debug('is returned deferred was called? %s' % d.called)
        d.addBoth(finish_request)
        return NOT_DONE_YET
    return wrapper


class Root(resource.Resource):
    isLeaf = True

    def __init__(self, lock):
        self._lock = lock
        self.log = Logger('web')
        resource.Resource.__init__(self)


    @delayed
    def render_GET(self, request):
        try:
            key = _get_key_from_path(request.path)
            if key == 'info/keys':
                request.write('%r\n' % (self._lock._keys,))
            elif key == 'info/status':
                for line in self._lock.get_status():
                    request.write('%s %s\n' % line)
            elif key == 'info/log':
                for line in self._lock._log:
                    request.write('%s\n' % line)
            else:
                value = yield self._lock.get_key(key)
                request.write(value)
        except KeyNotFound:
            request.setResponseCode(NOT_FOUND)
        returnValue('')


    @delayed
    def render_POST(self, request):
        try:
            self.log.debug('BLAH 1')
            key = _get_key_from_path(request.path)
            self.log.debug('BLAH 2')
            data = request.args.get('data', [''])[0]

            self.log.debug('BLAH 3')
            self.log.info('Set key %s=%r' % (key, data))
            self.log.debug('BLAH 4')
            yield self._lock.set_key(key, data)
            self.log.debug('BLAH 5')
        except KeyAlreadyExists, e:
            self.log.warning(e)
            request.setResponseCode(CONFLICT)
        except PaxosError, e:
            self.log.warning(e)
            request.setResponseCode(EXPECTATION_FAILED)
        except Exception, e:
            self.log.exception('SOME OTHER EXCEPTION')


    @delayed
    def render_DELETE(self, request):
        key = _get_key_from_path(request.path)
        try:
            self.log.info('Del key %s' % key)
            yield self._lock.del_key(key)
        except KeyNotFound, e:
            self.log.warning(e)
            request.setResponseCode(NOT_FOUND)
        except PaxosError, e:
            self.log.warning(e)
            request.setResponseCode(EXPECTATION_FAILED)
        except Exception, e:
            self.log.exception('SOME OTHER EXCEPTION')


