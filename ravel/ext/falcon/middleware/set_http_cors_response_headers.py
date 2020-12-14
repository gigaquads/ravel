import falcon

from typing import Dict, Tuple, Type, Text, List
from inspect import Parameter

from ravel.app.middleware import Middleware
from ravel.apps.web import Endpoint
from ravel.app.base import ActionDecorator
from ravel.ext.falcon.service import FalconService
from ravel.ext.falcon.constants import HTTP_METHODS, HTTP_OPTIONS

DEFAULT_ALLOW_ORIGIN = '*'
DEFAULT_ALLOW_METHODS = HTTP_METHODS
DEFAULT_ALLOW_HEADERS = (
    'Authorization',
    'Content-Type',
    'Accept',
    'Origin',
    'User-Agent',
    'DNT',
    'Cache-Control',
    'X-Mx-ReqToken',
    'Keep-Alive',
    'X-Requested-With',
    'If-Modified-Since',
    'Pragma',
    'Expires',
)


class SetHttpCorsResponseHeaders(Middleware):
    """
    # CORS Middleware

    This middleware sets various Access-Control headers needed for CORS.
    """

    def __init__(
        self,
        allow_origin: Text = DEFAULT_ALLOW_ORIGIN,
        allow_headers: List[Text] = DEFAULT_ALLOW_HEADERS,
        allow_methods: List[Text] = DEFAULT_ALLOW_METHODS,
    ):
        self._allow_origin = allow_origin
        self._allow_headers = allow_headers
        self._allow_methods = allow_methods
        self._cors_headers = {
            'Access-Control-Allow-Origin': self._allow_origin,
            'Access-Control-Allow-Methods': ','.join(self._allow_methods),
            'Access-Control-Allow-Headers': ','.join(self._allow_headers),
        }

    @property
    def app_types(self) -> Tuple[Type['Application']]:
        return (FalconService, )

    def on_bootstrap(self):
        """
        Ensure that every registered route has at least a dummy endpoint
        registered for the OPTIONS HTTP method, adding endpoints as needed.
        """
        for res in self.app.falcon_resources.values():
            if not res.is_method_supported(HTTP_OPTIONS):
                endpoint = Endpoint.from_function(
                    app=self.app,
                    func=lambda *args, **kwargs: None,
                    method=HTTP_OPTIONS,
                    route=res.route,
                )
                res.add_endpoint(endpoint)

    def pre_request(
        self,
        action: 'Action',
        request: 'Request',
        raw_args: Tuple,
        raw_kwargs: Dict
    ):
        """
        Set CORS headers on OPTIONS requests as well as tell Falcon and Ravel
        to abort both further middleware processing and the target/respondor
        method, going right to post-request middleware instead.
        """
        falcon_request, falcon_response = raw_args[:2]
        falcon_response.set_headers(self._cors_headers)