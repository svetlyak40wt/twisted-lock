import os
import twisted
import logging

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


def init_logging():
    _srcfile = twisted.python.log.__file__
    if _srcfile[-4:].lower() in ['.pyc', '.pyo']:
        _srcfile = _srcfile[:-4] + '.py'
    _srcfile = os.path.normcase(_srcfile)
    logging._srcfile = _srcfile

    logging.basicConfig(
        level = logging.DEBUG,
        format = '%(asctime)s %(process)s/%(thread)s %(levelname)s %(name)s %(filename)s:%(lineno)s %(message)s',
    )
    observer = twisted.python.log.PythonLoggingObserver()
    twisted.python.log.startLoggingWithObserver(observer.emit)

