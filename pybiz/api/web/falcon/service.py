from __future__ import absolute_import

import traceback

import falcon

from typing import Dict

from appyratus.decorators import memoized_property
from appyratus.io import Environment

from pybiz.api.wsgi import WsgiServiceFunctionRegistry

from .resource import ResourceManager
from .middleware import Middleware


class FalconServiceFunctionRegistry(WsgiServiceFunctionRegistry):

    env = Environment()

    class Request(falcon.Request):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.session = None
            self.json = {}

    class Response(falcon.Response):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        @memoized_property
        def ok(self):
            status_code = int(self.status[:3])
            return (200 <= status_code < 300)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._resource_manager = ResourceManager()
        self._url_path2resource = {}

    @property
    def middleware(self):
        return []

    @property
    def request_type(self):
        return self.Request

    @property
    def response_type(self):
        return self.Response

    @staticmethod
    def handle_error(exc, req, resp, params):
        resp.status = falcon.HTTP_500
        traceback.print_exc()

    def start(self, environ=None, start_response=None, *args, **kwargs):
        middleware = self.middleware
        for m in middleware:
            if isinstance(m, Middleware):
                m.bind(self)

        falcon_api = falcon.API(
            middleware=middleware,
            request_type=self.request_type,
            response_type=self.response_type,
        )

        falcon_api.add_error_handler(Exception, self.handle_error)

        for url_path, resource in self._url_path2resource.items():
            falcon_api.add_route(url_path, resource)

        return falcon_api(environ, start_response)

    def on_decorate(self, route):
        resource = self._resource_manager.add_route(route)
        if resource:
            self._url_path2resource[route.url_path] = resource

    def on_request(self, route, signature, req, resp, *args, **kwargs):
        api_kwargs = dict(req.json, session=req.session)
        api_kwargs.update(req.params)

        if route.authorize is not None:
            route.authorize(req, resp)

        # append URL path variables to positional args list
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
