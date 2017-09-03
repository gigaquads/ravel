import ujson

from falcon import HTTPError, status_codes


class PybizFalconError(HTTPError):

    http_status = status_codes.HTTP_400
    api_status = 'bad-request'
    default_message = 'bad request'

    def __init__(self, message=None, data=None):
        super().__init__()
        self.message = message or self.default_message
        self.data = data or {}

    def __repr__(self):
        return '<FalconError({})>'.format(self.__class__.__name__)

    def to_dict(self):
        return {
            'code': self.api_status,
            'status': self.http_status,
            'message': self.message,
            'data': self.data,
            }

    def to_json(self):
        return ujson.dumps(self.to_dict())


class NotFound(PybizFalconError):

    http_status = status_codes.HTTP_404
    api_status = 'not-found'
    default_message = 'resource not found'

    def __init__(self, resource_name, data=None):
        super().__init__(
            message='{} not found'.format(resource_name),
            data=data
            )
