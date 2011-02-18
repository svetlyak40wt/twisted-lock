import os
import twisted
import logging
import types

from functools import wraps

from twisted.internet.defer import Deferred
from twisted.internet import reactor

def parse_ip(text):
    text = text.strip()
    if ':' in text:
        host, port = text.rsplit(':', 1)
    elif '.' in text:
        host, port = text, 0
    else:
        host, port = '', text
    return host or '127.0.0.1', int(port)


def parse_ips(text):
    return map(parse_ip, text.split(','))


def init_logging(config):
    _srcfile = twisted.python.log.__file__
    if _srcfile[-4:].lower() in ['.pyc', '.pyo']:
        _srcfile = _srcfile[:-4] + '.py'
    _srcfile = os.path.normcase(_srcfile)

    if logging._srcfile != _srcfile:
        logging._srcfile = _srcfile

        logging.basicConfig(
            filename = config.get('logging', 'filename') or None,
            level = logging.DEBUG,
            format = '%(asctime)s %(process)s/%(thread)s %(levelname)s %(name)s %(filename)s:%(lineno)s %(message)s',
        )
        observer = twisted.python.log.PythonLoggingObserver()
        twisted.python.log.startLoggingWithObserver(observer.emit)


def trace(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        log = logging.getLogger('trace')
        log.info('Calling %s(args = %r, kwargs = %r)' % (
            func.__name__, args, kwargs
        ))
        return func(*args, **kwargs)
    return wrapper


def trace_all(cls):
    for key in dir(cls):
        if not key.startswith('__') or key == '__init__':
            value = getattr(cls, key)
            if isinstance(value, (types.MethodType, types.FunctionType)):
                setattr(cls, key, trace(value))
    return cls


escape = lambda x: x.replace('\\', '\\\\').replace('"', '\\"')


def wait_calls(predicate, check_step = 0.1):
    """Async waiter for predicate.
    Returns deferred and will call callback
    when predicate() will return true.
    """
    d = Deferred()
    def _wait():
        if predicate():
            d.callback(True)
        else:
            reactor.callLater(check_step, _wait)
    reactor.callLater(check_step, _wait)
    return d


