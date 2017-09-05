class Authenticator(object):
    pass


class AuthenticatorObject(object):
    pass


class AccessToken(AuthenticatorObject):

    def __init__(self, value, expires_at):
        self.value = value
        self.expires_at = expires_at


class RemoteUser(AuthenticatorObject):

    def __init__(self, user_id, email=None):
        self.user_id = user_id
        self.email = email
