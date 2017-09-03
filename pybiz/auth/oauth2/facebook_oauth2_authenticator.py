import requests

from datetime import timedelta
from abc import ABCMeta, abstractmethod
from urllib.parse import urlencode

from pybiz.util import Environment, utc_now
from pybiz import Schema, fields

from .oauth2_authenticator import Oauth2Authenticator, AccessToken, RemoteUser


class FacebookEnvironment(Environment):

    client_id = fields.Str(load_from='fb_client_id', required=True)
    client_secret = fields.Str(load_from='fb_client_secret', required=True)
    redirect_uri = fields.Str(load_from='fb_redirect_uri', required=True)


class FacebookOauth2Authenticator(Oauth2Authenticator):

    _env = FacebookEnvironment.get_instance()
    _login_url = 'https://www.facebook.com/v2.10/dialog/oauth'
    _access_token_url = 'https://graph.facebook.com/v2.10/oauth/access_token'
    _scope = ('public_profile', 'email')
    _user_fields = ('name', 'email')

    def __init__(self, csrf_token):
        super().__init__(csrf_token)

    @property
    def redirect_url(self):
        return self._env.redirect_uri

    @property
    def login_url(self):
        return '{base_url}?{query_params}'.format(
            base_url=self._login_url,
            query_params=urlencode({
                'client_id': self._env.client_id,
                'redirect_uri': self.redirect_url,
                'state': self.csrf_token,
                'scope': ','.join(self._scope)
                }))

    def request_access_token(self, auth_code):
        resp = requests.get(self._access_token_url, params={
            'client_id': self._env.client_id,
            'client_secret': self._env.client_secret,
            'redirect_uri': self.redirect_url,
            'code': auth_code
            })

        resp.raise_for_status()
        data = resp.json()

        return AccessToken(
            value=data['access_token'],
            expires_at=utc_now() + timedelta(seconds=data['expires_in']),
            )

    def request_user(self, access_token):
        resp = requests.get(
            url='https://graph.facebook.com/me',
            params={
                'access_token': access_token,
                'fields': ','.join(self._user_fields)
                })

        resp.raise_for_status()
        data = resp.json()

        return RemoteUser(
            name=data['name'],
            email=data['email'],
            )
