from __future__ import absolute_import

import traceback

import falcon

from appyratus.env import Environment

from ravel.app.middleware import Middleware
from ravel.apps.web import AbstractWsgiService
from ravel.util.json_encoder import JsonEncoder
from ravel.util.loggers import console

from .media import JsonHandler
from .resource import FalconResource


class FalconService(AbstractWsgiService):

    env = Environment()

    def __init__(self, router=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._json_encoder = JsonEncoder()
        self._route_2_resource = {}
        self._router = router

    @property
    def falcon_middleware(self):
        return []

    @property
    def falcon_resources(self):
        return self._route_2_resource

    @property
    def request_type(self):
        return Request

    @property
    def response_type(self):
        return Response

    @staticmethod
    def handle_error(exc, request, response, params):
        response.status = falcon.HTTP_500
        traceback.print_exc()

    def on_start(self, *args, **kwargs):
        console.info(
            message='starting Falcon web service',
            data={'endpoints': sorted(self.actions.keys())}
        )
        return self.entrypoint(*args, **kwargs)

    def entrypoint(
        self, environ=None, start_response=None, *args, **kwargs
    ):
        middleware = self.falcon_middleware
        for m in middleware:
            if isinstance(m, Middleware):
                m.bind(self)

        falcon_app = falcon.App(
            middleware=middleware,
            request_type=self.request_type,
            response_type=self.response_type,
            router=self._router
        )
        falcon_app.req_options = Request.Options()
        falcon_app.resp_options = Response.Options()
        falcon_app.add_error_handler(Exception, self.handle_error)

        for route, resource in self._route_2_resource.items():
            falcon_app.add_route(route, resource)

        return falcon_app

    def on_decorate(self, endpoint):
        super().on_decorate(endpoint)
        for route in endpoint.routes:
            resource = self._route_2_resource.get(route)
            if resource is None:
                resource = FalconResource(route)
                self._route_2_resource[route] = resource
                resource.add_endpoint(endpoint)
            else:
                resource.add_endpoint(endpoint)

    def on_request(self, endpoint, request, response, *args, **kwargs):
        if request.content_length:
            app_kwargs = dict(request.media or {}, **kwargs)
        else:
            app_kwargs = dict(kwargs)

        app_kwargs.update(request.params)

        # append URL path variables
        route = request.path.strip('/').split('/')
        route_template = request.uri_template.strip('/').split('/')
        for k, v in zip(route_template, route):
            if k and (k[0] == '{' and k[-1] == '}'):
                app_kwargs[k[1:-1]] = v

        return (tuple(), app_kwargs)

    def on_response(
        self,
        action,
        result,
        raw_args=None,
        raw_kwargs=None,
        *args,
        **kwargs
    ):
        request, response = raw_args
        response.media = result
        return result


class Request(falcon.Request):
    """
    Specialized HTTP Request object (NOT a Ravel Request type)
    """

    class Options(falcon.RequestOptions):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.media_handlers['application/json'] = JsonHandler()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = None
        self.json = {}


class Response(falcon.Response):
    class Options(falcon.ResponseOptions):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.media_handlers['application/json'] = JsonHandler()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def ok(self):
        status_code = int(self.status[:3])
        return (200 <= status_code < 300)
