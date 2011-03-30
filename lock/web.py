# -*- coding: utf-8 -*-
from __future__ import absolute_import

from urlparse import urlparse
from functools import wraps

from logbook import Logger
from twisted.web import resource
from twisted.web.http import CONFLICT, NOT_FOUND, INTERNAL_SERVER_ERROR, EXPECTATION_FAILED
from twisted.web.server import NOT_DONE_YET
from twisted.web.proxy import ProxyClientFactory
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue
from twisted.internet.error import ConnectionDone
from twisted.internet.task import deferLater
from twisted.internet import reactor
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
        was_interrupted = [False]
        log = Logger('web')

        def on_cancel(failure):
            err(failure, 'Call to "%s" was interrupted' % request.path)
            was_interrupted[0] = True

        request.notifyFinish().addErrback(on_cancel)

        def finish_request(result):
            log.debug('%s(%r, args=%r, kwargs=%r)=%s' % (func.__name__, request, args, kwargs, result))

            if isinstance(result, Failure):
                request.setResponseCode(INTERNAL_SERVER_ERROR)
                log.exception('Call to %s(%r, args=%r, kwargs=%r) failed' % (func.__name__, request, args, kwargs), exc_info = (result.type, result.value, result.getTracebackObject()))

            if was_interrupted[0] == False and result != NOT_DONE_YET:
                request.finish()

        log.debug('Calling %s(%r, args=%r, kwargs=%r)' % (func.__name__, request, args, kwargs))
        d = func(self, request, *args, **kwargs)
        log.debug('Result: %s' % d)
        log.debug('is returned deferred was called? %s' % d.called)
        d.addBoth(finish_request)
        return NOT_DONE_YET
    return wrapper


def proxy_to_master(func):
    @wraps(func)
    def wrapper(self, request, *args, **kwargs):
        if self._lock.master is not None and \
           self._lock.master.http != (self._lock.http_interface, self._lock.http_port):

            def try_to_do_this_myself(reason):
                self.log.error('Proxy request failed with message: "%s", trying to run request locally.' % (reason,))
                self._lock.master = None
                return func(self, request, *args, **kwargs)

            proxy_factory = self._proxy(request)
            proxy_factory.done.addErrback(try_to_do_this_myself)
            return NOT_DONE_YET
        else:
            return func(self, request, *args, **kwargs)
    return wrapper


class LockProxyClientFactory(ProxyClientFactory):
    def __init__(self, *args, **kwargs):
        ProxyClientFactory.__init__(self, *args, **kwargs)
        self.done = Deferred()

    def clientConnectionLost(self, connector, reason):
        if reason.type != ConnectionDone:
            self.done.errback(reason)
        self.done.callback(True)

    def clientConnectionFailed(self, connector, reason):
        self.done.errback(reason)


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


    @proxy_to_master
    @delayed
    def render_POST(self, request):
        try:
            key = _get_key_from_path(request.path)
            data = request.args.get('data', [''])[0]

            self.log.info('Set key %s=%r' % (key, data))
            yield self._lock.set_key(key, data)
        except KeyAlreadyExists, e:
            self.log.warning(e)
            request.setResponseCode(CONFLICT)
        except PaxosError, e:
            self.log.warning(e)
            request.setResponseCode(EXPECTATION_FAILED)
        except Exception, e:
            self.log.exception('SOME OTHER EXCEPTION')


    @proxy_to_master
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


    def _proxy(self, request):
        """
        Render a request by forwarding it to the proxied server.
        """
        # RFC 2616 tells us that we can omit the port if it's the default port,
        # but we have to provide it otherwise
        host, port = self._lock.master.http

        request.received_headers['host'] = '%s:%s' % (host, port)
        request.content.seek(0, 0)
        qs = urlparse(request.uri)[4]
        if qs:
            rest = request.path + '?' + qs
        else:
            rest = request.path
        self.log.debug('Proxing %s %s to the master at %s:%s' %
            (request.method, rest, host, port)
        )
        client_factory = LockProxyClientFactory(
            request.method, rest, request.clientproto,
            request.getAllHeaders(), request.content.read(), request)
        reactor.connectTCP(host, port, client_factory)
        return client_factory

