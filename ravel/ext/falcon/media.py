from __future__ import absolute_import

import six

from falcon import errors
from falcon.media import BaseHandler
from falcon.util import json

from ravel.util.json_encoder import JsonEncoder


class JsonHandler(BaseHandler):
    """
    # Json Handler
    """

    def __init__(self, encoder: 'JsonEncoder' = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.encoder = encoder or JsonEncoder()

    def deserialize(self, stream, content_type, content_length):
        try:
            raw = stream.read()
            return self.encoder.decode(raw.decode('utf-8'))
        except ValueError as err:
            raise errors.HTTPBadRequest(
                'Invalid JSON',
                'Could not parse JSON body - {0}'.format(err)
            )

    def serialize(self, media, content_type):
        result = self.encoder.encode(media)
        if six.PY3 or not isinstance(result, bytes):
            result = result.encode('utf-8')
        return result
