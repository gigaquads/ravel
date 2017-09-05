import requests

from datetime import timedelta
from abc import ABCMeta, abstractmethod
from urllib.parse import urlencode

from pybiz.util import utc_now
from pybiz import Schema, fields

from ..authenticator import Authenticator, AccessToken, RemoteUser


class Oauth2Authenticator(Authenticator, metaclass=ABCMeta):

    def __init__(self, csrf_token=None):
        self._csrf_token = csrf_token

    @property
    def csrf_token(self):
        return self._csrf_token

    @property
    @abstractmethod
    def redirect_url(self) -> str:
        """
        Return the URL to which the oauth provider redirects after the user has
        submits their credentials.
        """

    @property
    @abstractmethod
    def login_url(self) -> str:
        """
        Return a URL to the provider's login page, where the user submits his
        credentials and gets redirected to the redirect URL.
        """

    @abstractmethod
    def request_access_token(self, auth_code) -> AccessToken:
        """
        Sends a request to the provider using the auth code that came back to
        the redirect URL.
        """

    @abstractmethod
    def request_user(self, access_token) -> RemoteUser:
        """
        Sends a request to the provider for basic user information.
        """
