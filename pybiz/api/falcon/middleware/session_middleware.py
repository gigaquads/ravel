from typing import Text

from pybiz.biz import BizObject

from .base import Middleware


class AbstractSessionMiddleware(Middleware):

    DEFAULT_COOKIE_NAME = 'sid'

    def __init__(self, cookie_name: Text = None):
        super().__init__()
        self._cookie_name = cookie_name or self.DEFAULT_COOKIE_NAME

    def fetch_session(self, session_key: Text) -> BizObject:
        """
        Responsibility of implementer to fetch a Session object from the DAL,
        using the session key retreived from the HTTP session cookie.
        """
        raise NotImplementedError('override in subclass')

    def create_session(self, request, response) -> Text:
        """
        Responsibility of implementer to create a Session BizObject,
        returning the string session key to set in the response cookie.
        """
        raise NotImplementedError('override in subclass')

    def process_request(self, request, response):
        key = request.cookies.get(self._cookie_name)
        if key is not None:
            session = self.fetch_session(key)
            request.session = session

    def process_resource(self, request, response, resource, params):
        pass

    def process_response(self, request, response, resource):
        request.session = None
        session_key = self.create_session(request, response)
        if session_key is not None:
            response.set_cookie(self._cookie_name, session_key)
