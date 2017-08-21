import ujson


class JsonTranslator(object):

    def __init__(self, encoder=None):
        self.encoder = encoder or ujson

    def process_request(self, req, resp):
        if req.content_length:
            req.json = ujson.loads(req.stream.read().decode())
        else:
            req.json = {}

    def process_resource(self, req, resp, resource, params):
        pass

    def process_response(self, req, resp, resource):
        if resp.body is not None:
            resp.body = self.encoder.encode(resp.body)
        else:
            resp.body = '{}'
