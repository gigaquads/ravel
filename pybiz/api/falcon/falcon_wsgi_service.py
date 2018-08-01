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

    @property
    def middleware(self):
        return []

    @property
    def request_type(self):
        return self.Request

    def start(self, environ=None, start_response=None, *args, **kwargs):
        return self.falcon_api(environ, start_response)

    def on_decorate(self, route):
        resource = self.resource_manager.add_route(route)
        if resource:
            self.falcon_api.add_route(route.url_path, resource)

    def on_request(self, signature, req, resp, *args, **kwargs) -> Dict:
        api_kwargs = req.json.copy()
        api_kwargs.update(req.params)
        return (args, api_kwargs)

    def on_response(self, result, request, response, *args, **kwargs):
        # The `result` object needs to be serialized by middleware.
        response.unserialized_body = result
