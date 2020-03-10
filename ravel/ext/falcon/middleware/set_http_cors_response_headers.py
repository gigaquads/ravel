import falcon

from typing import Dict, Tuple, Type, Text, List
from inspect import Parameter

from ravel.app.middleware import Middleware
from ravel.ext.falcon.service import FalconService
from ravel.ext.falcon.constants import HTTP_METHODS

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
        allow_origin: Text = None,
        allow_headers: List[Text] = None,
        allow_methods: List[Text] = None,
    ):
        self._allow_origin = allow_origin or DEFAULT_ALLOW_ORIGIN
        self._allow_headers = allow_headers or DEFAULT_ALLOW_HEADERS
        self._allow_methods = allow_methods or DEFAULT_ALLOW_METHODS

    @property
    def app_types(self) -> Tuple[Type['Application']]:
        return (FalconService, )

    def post_request(
        self,
        action: 'Action',
        request: 'Request',
        result
    ):
        falcon_request, falcon_response = request.raw_args[:2]
        falcon_response.set_headers(
            {
                'Access-Control-Allow-Origin': self._allow_origin,
                'Access-Control-Allow-Methods': ','.join(self._allow_methods),
                'Access-Control-Allow-Headers': ','.join(self._allow_headers),
            }
        )
