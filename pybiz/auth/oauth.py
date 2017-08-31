from abc import ABCMeta, abstractmethod


class OauthSession(object, metaclass=ABCMeta):

    def __init__(self, csrf_token):
        self.csrf_token = csrf_token

    @abstractmethod
    def get_redirect_url(self):
        """
        Return a string redirect URL, to which the Oauth provider should
        redirect after the user has entered ethier credentials.
        """

    @abstractmethod
    def get_oauth_login_dialog_url(self):
        """
        Return a URL to the provider's login page, where the user enters his
        credentials and gets redirected to the redirect URL.
        """

    @abstractmethod
    def request_access_token(self, auth_code):
        """
        Sends a request to the provider using the auth code that came back to
        the redirect URL.
        """

    @abstractmethod
    def request_user_info(self, access_token):
        """
        Sends a request to the provider for basic user information.
        """


class FacebookOauthSession(OauthSession):

    _base_redirect_url = ''
    _base_access_token_url = ''

    def __init__(self, client_id, client_secret):
        self._client_id = client_id
        self._client_secret = client_secret

    def get_redirect_url(self):
        pass

    def get_oauth_login_dialog_url(self):
        pass

    def request_access_token(self, auth_code):
        pass

    def request_user_info(self, access_token):
        pass
