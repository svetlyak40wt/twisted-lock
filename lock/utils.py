import logbook
import sys
import twisted
import types

from functools import wraps
from twisted.python.log import textFromEventDict, addObserver, removeObserver
from twisted.internet.defer import Deferred
from twisted.internet import reactor

_logging_initialized = False

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


class Logger(logbook.Logger):
    def handle(self, record):
        record.frame = sys._getframe(6)
        return super(Logger, self).handle(record)


class LogbookObserver(object):
    """Output twisted messages to logbook."""

    def __init__(self, logger_name="twisted"):
        self.logger = Logger(logger_name)

    def emit(self, event_dict):
        if 'logLevel' in event_dict:
            level = event_dict['logLevel']
        elif event_dict['isError']:
            level = logbook.ERROR
        else:
            level = logbook.INFO

        text = textFromEventDict(event_dict)
        if text is None:
            return

        self.logger.log(level, text)

    def start(self):
        """
        Start observing log events.
        """
        addObserver(self.emit)

    def stop(self):
        """
        Stop observing log events.
        """
        removeObserver(self.emit)


def init_logging(config):
    global _logging_initialized
    if not _logging_initialized:
        handler = logbook.FileHandler(
            config.get('logging', 'filename') or 'twisted-lock.log',
            format_string='[{record.time}] {record.level_name:>5} {record.extra[node]} {record.module}:{record.lineno} {record.message}',
        )
        handler.push_application()

        observer = LogbookObserver()
        twisted.python.log.startLoggingWithObserver(observer.emit, setStdout = False)
        _logging_initialized = True


def trace(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        log = Logger('trace')
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


