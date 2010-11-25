from twisted.web import resource

class Root(resource.Resource):
    isLeaf = True
    def __init__(self, lock):
        self._lock = lock
        resource.Resource.__init__(self)

    def render_GET(self, request):
        return "Hello, world!"

