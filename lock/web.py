from __future__ import absolute_import

from twisted.web import resource
from twisted.web.http import CONFLICT, NOT_FOUND

from . exceptions import KeyAlreadyExists, KeyNotFound

def _get_key(path):
    return path[1:]

class Root(resource.Resource):
    isLeaf = True

    def __init__(self, lock):
        self._lock = lock
        resource.Resource.__init__(self)


    def render_GET(self, request):
        key = _get_key(request.path)
        try:
            return self._lock.get_key(key)
        except KeyNotFound:
            request.setResponseCode(NOT_FOUND)
        return ''


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


