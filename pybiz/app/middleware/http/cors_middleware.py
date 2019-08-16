import falcon

from typing import Dict, Tuple, Type, Text
from inspect import Parameter
from pybiz.contrib.falcon.constants import HTTP_METHODS

from ..application_middleware import ApplicationMiddleware

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


class CorsMiddleware(ApplicationMiddleware):
    """
    CORS
    """

    @property
    def app_types(self) -> Tuple[Type['Application']]:
        from pybiz.contrib.falcon import FalconService
        return (FalconService, )

    def post_request(
        self, endpoint: 'Endpoint', raw_args: Tuple, raw_kwargs: Dict, *args, **kwargs
    ):
        request, response = raw_args
        response.set_headers(
            {
                'Access-Control-Allow-Origin': DEFAULT_ALLOW_ORIGIN,
                'Access-Control-Allow-Methods': ','.join(DEFAULT_ALLOW_METHODS),
                'Access-Control-Allow-Headers': ','.join(DEFAULT_ALLOW_HEADERS),
            }
        )

    def get_request(self, raw_args, raw_kwargs):
        return raw_args[0]
