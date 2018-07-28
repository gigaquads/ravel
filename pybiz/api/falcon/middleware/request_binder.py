class RequestBinder(object):
    def __init__(self, objects: list = None):
        self._objects = objects

    def process_request(self, req, resp):
        for obj in self._objects:
            setattr(obj, 'request', req)
            setattr(obj, 'response', resp)

    def process_resource(self, req, resp, resource, params):
        pass

    def process_response(self, req, resp, resource):
        for obj in self._objects:
            delattr(obj, 'request')
            delattr(obj, 'response')
