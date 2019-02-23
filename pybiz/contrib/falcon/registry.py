from __future__ import absolute_import

import traceback

import falcon

from typing import Dict

from appyratus.memoize import memoized_property
from appyratus.env import Environment

from pybiz.api.web import WsgiServiceRegistry
from pybiz.util import JsonEncoder

from .resource import ResourceManager
from .media import JsonHandler


class FalconServiceRegistry(WsgiServiceRegistry):

    env = Environment()

    class Request(falcon.Request):
        class Options(falcon.RequestOptions):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.media_handlers['application/json'] = JsonHandler()

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
        self._json_encoder = JsonEncoder()
        self._resource_manager = ResourceManager()
        self._url_path2resource = {}

    @property
    def falcon_middleware(self):
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

    def start(self):
        return self.entrypoint

    def entrypoint(self, environ=None, start_response=None, *args, **kwargs):
        middleware = self.falcon_middleware
        for m in middleware:
            if isinstance(m, Middleware):
                m.bind(self)

        falcon_api = falcon.API(
            middleware=middleware,
            request_type=self.request_type,
            response_type=self.response_type,
        )
        falcon_api.req_options = self.Request.Options()
        falcon_api.add_error_handler(Exception, self.handle_error)

        for url_path, resource in self._url_path2resource.items():
            falcon_api.add_route(url_path, resource)

        return falcon_api(environ, start_response)

    def on_decorate(self, route):
        resource = self._resource_manager.add_route(route)
        if resource:
            self._url_path2resource[route.url_path] = resource

    def on_request(self, route, req, resp, *args, **kwargs):
        if req.content_length:
            api_kwargs = dict(req.media or {}, **kwargs)
        else:
            api_kwargs = dict(kwargs)

        api_kwargs.update(req.params)

        if route.authorize is not None:
            route.authorize(req, resp)

        # append URL path variables
        url_path = req.path.strip('/').split('/')
        url_path_template = req.uri_template.strip('/').split('/')
        for k, v in zip(url_path_template, url_path):
            if k[0] == '{' and k[-1] == '}':
                api_kwargs[k[1:-1]] = v

        #api_args.extend(args)
        return (tuple(), api_kwargs)

    def on_response(self, route, result, request, response, *args, **kwargs):
        response.media = result
