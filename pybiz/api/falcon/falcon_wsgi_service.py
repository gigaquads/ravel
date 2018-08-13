from __future__ import absolute_import

import traceback

import falcon

from typing import Dict

from pybiz.api.wsgi_service import WsgiService

from .resource import ResourceManager


class FalconWsgiService(WsgiService):

    class Request(falcon.Request):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.session = None
            self.json = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.resource_manager = ResourceManager()
        self.falcon_api = falcon.API(
            middleware=self.middleware,
            request_type=self.request_type
        )
        self.falcon_api.add_error_handler(
            Exception, self.handle_error
        )

    @property
    def middleware(self):
        return []

    @property
    def request_type(self):
        return self.Request

    @staticmethod
    def handle_error(exc, req, resp, params):
        resp.status = falcon.HTTP_500
        traceback.print_exc()

    def start(self, environ=None, start_response=None, *args, **kwargs):
        return self.falcon_api(environ, start_response)

    def on_decorate(self, route):
        resource = self.resource_manager.add_route(route)
        if resource:
            self.falcon_api.add_route(route.url_path, resource)

    def on_request(self, route, signature, req, resp, *args, **kwargs):
        api_kwargs = req.json.copy()
        api_kwargs.update(req.params)
        api_kwargs['session'] = req.session

        if route.authorize is not None:
            route.authorize(req, resp)

        api_args = []
        url_path = req.path.strip('/').split('/')
        url_path_template = req.uri_template.strip('/').split('/')
        for k, v in zip(url_path_template, url_path):
            if k[0] == '{' and k[-1] == '}':
                api_args.append(v)

        api_args.extend(args)
        return (api_args, api_kwargs)

    def on_response(self, route, result, request, response, *args, **kwargs):
        # The `result` object needs to be serialized by middleware.
        response.unserialized_body = result
