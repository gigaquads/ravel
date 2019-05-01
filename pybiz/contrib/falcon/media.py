from __future__ import absolute_import

import six
import ujson

from falcon import errors
from falcon.media import BaseHandler
from falcon.util import json

from pybiz.util import JsonEncoder


class JsonHandler(BaseHandler):
    """
    Handler built using the :py:mod:`ujson` module.
    """

    def __init__(self, encoder: 'JsonEncoder' = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.encoder = encoder or JsonEncoder()

    def deserialize(self, raw):
        try:
            return JsonEncoder.decode(raw.decode('utf-8'))
        except ValueError as err:
            raise errors.HTTPBadRequest(
                'Invalid JSON',
                'Could not parse JSON body - {0}'.format(err)
            )

    def serialize(self, media):
        result = ujson.dumps(media, ensure_ascii=False)
        if six.PY3 or not isinstance(result, bytes):
            result = result.encode('utf-8')
        return result
