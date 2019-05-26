import falcon

from typing import Dict, Tuple, Type, Text
from inspect import Parameter

from ..base import RegistryMiddleware

DEFAULT_ALLOW_ORIGIN = '*'
DEFAULT_ALLOW_METHODS = falcon.HTTP_METHODS
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


class CorsMiddleware(RegistryMiddleware):
    """
    CORS
    """

    @property
    def registry_types(self) -> Tuple[Type['Registry']]:
        from pybiz.contrib.falcon import FalconServiceRegistry
        return (FalconServiceRegistry, )

    def post_request(
        self, proxy: 'RegistryProxy', raw_args: Tuple, raw_kwargs: Dict,
        *args, **kwargs
    ):
        request, response = raw_args
        response.set_headers(
            {
                'Access-Control-Allow-Origin': DEFAULT_ALLOW_ORIGIN,
                'Access-Control-Allow-Methods': DEFAULT_ALLOW_METHODS,
                'Access-Control-Allow-Headers': DEFAULT_ALLOW_HEADERS,
            }
        )

    def get_request(self, raw_args, raw_kwargs):
        return raw_args[0]
