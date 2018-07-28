from __future__ import absolute_import

import falcon

from typing import Dict

from pybiz.api.wsgi_service import WsgiService

from .resource import ResourceManager


class FalconWsgiService(WsgiService):

    class Request(falcon.Request):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.json = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.resource_manager = ResourceManager()
        self.falcon_api = falcon.API(
            middleware=self.middleware,
            request_type=self.request_type
        )

    def start(self, environ=None, start_response=None, *args, **kwargs):
        return self.falcon_api(environ, start_response)

    def on_decorate(self, route):
        resource = self.resource_manager.add_route(route)
        self.falcon_api.add_route(route.url_path, resource)

    def on_request(self, signature, request, response, *args, **kwargs) -> Dict:
        args = ()
        kwargs = {
            'request': request,
            'response': response,
        }
        kwargs.update(request.json)
        kwargs.update(request.params)
        return (args, kwargs)

    def on_response(self, result, request, response):
        # The `result` object needs to be serialized by middleware.
        response.unserialized_body = result

    @property
    def middleware(self):
        return []

    @property
    def request_type(self):
        return self.Request
