import ujson

from pybiz.util import is_bizobj


class JsonTranslator(object):

    def __init__(self, encoder=None):
        self.encoder = encoder or ujson

    def process_request(self, request, response):
        if request.content_length:
            request.json = ujson.loads(request.stream.read().decode())
        else:
            request.json = {}

    def process_resource(self, request, response, resource, params):
        if resource is None:
            return

        # get the ApiHandler instance associated with this request
        handler = resource.get_handler(request=request)
        if handler is None:
            return

        # apply validation to request JSON body
        if 'request' in handler.schemas:
            schema = handler.schemas['request']
            request.json = schema.load(request.json).data

        # apply validation to request query string params
        # replace the params objects from falcon with
        # schema-loaded data
        if 'params' in handler.schemas:
            schema = handler.schemas['params']
            result = schema.load(params).data
            params.clear()
            params.update(result.data)
            request.params = params

    def process_response(self, request, response, resource):
        if response.body is not None:
            if resource is not None:
                handler = resource.get_handler(request=request)
                if handler is not None:
                    schema = handler.schemas.get('response')
                    if schema is not None:
                        if is_bizobj(response.body):
                            result = schema.dump(response.body.dump())
                        else:
                            result = schema.dump(response.body)
                        response.body = result.data
                    elif is_bizobj(response.body):
                        response.body = response.body.dump()

            # encode the body object to a JSON string
            response.body = self.encoder.encode(response.body)
        else:
            response.body = self.encoder.encode({})
