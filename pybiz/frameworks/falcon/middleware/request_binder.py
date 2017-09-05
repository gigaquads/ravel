import threading


class RequestBinder(object):

    def __init__(self, objects: list = None):
        self._local = threading.local()
        self._local.is_bound = False
        self._objects = objects

    def process_request(self, req, resp):
        self._local.request = req
        self._local.response = resp
        if not self._local.is_bound:
            self._local.is_bound = True
            for obj in self._objects:
                setattr(obj, 'request', self._local.request)
                setattr(obj, 'response', self._local.response)

    def process_resource(self, req, resp, resource, params):
        pass

    def process_response(self, req, resp, resource):
        self._local.request = None
        self._local.response = None
