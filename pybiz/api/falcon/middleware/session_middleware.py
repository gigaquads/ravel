from typing import Text

from falcon import HTTPUnauthorized, HTTPForbidden

from pybiz.biz import BizObject

from .base import Middleware


DEFAULT_SESSION_COOKIE_NAME = 'sid'


class AbstractSessionMiddleware(Middleware):

    def __init__(self, cookie_name: Text = None):
        self._session_cookie_name = cookie_name or DEFAULT_SESSION_COOKIE_NAME
        super().__init__()

    def get_session(self, session_key: Text) -> BizObject:
        """
        Responsibility of implementer to get a Session object from the DAL,
        using the session key retreived from the HTTP session cookie.
        """
        raise NotImplementedError('override in subclass')

    def new_session(self, request, response) -> Text:
        """
        Responsibility of implementer to create a Session BizObject,
        returning the string session key to set in the response cookie.
        """
        raise NotImplementedError('override in subclass')

    def is_authenticated(self, request, resource) -> bool:
        """
        Responsibility of implementer to determine whether a user session is
        required and, if so, whether it exists.
        """
        return True

    def is_authorized(self, request, resource) -> bool:
        """
        Responsibility of implementer to authorize the request and return a True
        value if the request is indeed authorized.
        """
        return True

    def process_request(self, request, response):
        key = request.cookies.get(self._cookie_name)
        if key is not None:
            session = self.get_session(key)
            request.session = session

    def process_resource(self, request, response, resource, params):
        if not self.is_authenticated(request, resource):
            raise HTTPUnauthorized()
        if not self.is_authorized(request, resource):
            raise HTTPForbidden()

    def process_response(self, request, response, resource):
        request.session = None
        session_key = self.new_session(request, response)
        if session_key is not None:
            response.set_cookie(self._cookie_name, session_key)
