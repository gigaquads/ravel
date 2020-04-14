from __future__ import absolute_import

from ravel.util.json_encoder import JsonEncoder

from falcon import HTTPError, status_codes

json = JsonEncoder()


class RavelFalconError(HTTPError):

    http_status = status_codes.HTTP_400
    api_status = 'bad-request'
    default_message = 'bad request'

    def __init__(self, message=None, data=None):
        self.data = data or {}
        self.message = message or self.default_message
        super().__init__(status=self.http_status, description=self.message)

    def __repr__(self):
        return '<FalconError({})>'.format(self.__class__.__name__)

    def to_dict(self):
        return {
            'status': self.http_status,
            'message': self.message,
            'data': self.data,
            }

    def to_json(self):
        return json.encode(self.to_dict())


class NotFound(RavelFalconError):

    http_status = status_codes.HTTP_404
    api_status = 'not-found'
    default_message = 'resource not found'

    def __init__(self, resource_name, data=None):
        super().__init__(
            message='{} not found'.format(resource_name),
            data=data
            )


class NotAuthenticated(RavelFalconError):

    http_status = status_codes.HTTP_401
    api_status = 'not-authenticated'
    default_message = 'unauthenticated'

    def __init__(self):
        super().__init__()
