import requests

from datetime import timedelta
from abc import ABCMeta, abstractmethod
from urllib.parse import urlencode

from pybiz.util import utc_now
from pybiz import Schema, fields


class AccessTokenSchema(Schema):
    access_token = fields.Str(required=True)
    expires_at = fields.DateTime(required=True)


class UserInfoSchema(Schema):
    email = fields.Email(required=True)
    name = fields.Str()


class OauthSession(object, metaclass=ABCMeta):

    def __init__(self, csrf_token):
        self.csrf_token = csrf_token
        self.access_token_schema = AccessTokenSchema(strict=True)
        self.user_info_schema = UserInfoSchema(strict=True)

    @abstractmethod
    def get_redirect_url(self) -> str:
        """
        Return the URL to which the oauth provider redirects after the user has
        submits their credentials.
        """

    @abstractmethod
    def get_login_dialog_url(self) -> str:
        """
        Return a URL to the provider's login page, where the user submits his
        credentials and gets redirected to the redirect URL.
        """

    @abstractmethod
    def request_access_token(self, auth_code) -> dict:
        """
        Sends a request to the provider using the auth code that came back to
        the redirect URL.
        """

    @abstractmethod
    def request_user_info(self, access_token) -> dict:
        """
        Sends a request to the provider for basic user information.
        """


class FacebookOauthSession(OauthSession):

    _base_redirect_url = 'https://www.facebook.com/v2.10/dialog/oauth?'
    _access_token_url = 'https://graph.facebook.com/v2.10/oauth/access_token'
    _redirect_uri = 'http://localhost'
    _scope = ('public_profile', 'email')
    _user_fields = ('name', 'email')

    def __init__(self, csrf_token, client_id, client_secret):
        super().__init__(csrf_token)
        self._client_id = client_id
        self._client_secret = client_secret

    def get_redirect_url(self):
        return self._redirect_uri

    def get_login_dialog_url(self):
        return '{base_url}{query_params}'.format(
            base_url=self._base_redirect_url,
            query_params=urlencode({
                'client_id': self._client_id,
                'redirect_uri': self.get_redirect_url(),
                'state': self.csrf_token,
                'scope': ','.join(self._scope)
                }))

    def request_access_token(self, auth_code):
        resp = requests.get(self._access_token_url, params={
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'redirect_uri': self.get_redirect_url(),
            'code': auth_code
            })
        resp.raise_for_status()
        data = resp.json()
        return self.access_token_schema.load({
            'access_token': data['access_token'],
            'expires_at': utc_now() + timedelta(seconds=data['expires_in'])
            }).data

    def request_user_info(self, access_token):
        url = 'https://graph.facebook.com/me'
        resp = requests.get(url, params={
            'access_token': access_token,
            'fields': ','.join(self._user_fields)
            })
        resp.raise_for_status()
        data = resp.json()
        return self.user_info_schema.load({
            'name': data['name'],
            'email': data['email']
            }).data
