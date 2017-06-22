import threading


class RequestBinder(object):

    def __init__(self, objs: list = None):
        self._local = threading.local()
        for obj in (objs or []):
            self._add_properties(obj)

    def process_request(self, req, resp):
        self._local.request = req
        self._local.response = resp

    def process_resource(self, req, resp, resource, params):
        pass

    def process_response(self, req, resp, resource):
        self._local.request = None
        self._local.response = None

    def _add_properties(self, obj):
        local = self._local
        for k in ['request', 'response']:
            setattr(obj, k, property(fget=lambda self: getattr(local, k)))
