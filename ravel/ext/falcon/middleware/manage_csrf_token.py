from base64 import b64decode, b64encode
from uuid import uuid4
from datetime import datetime, timedelta
from typing import Text, Tuple, Type, Dict

from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Util.Padding import pad, unpad

from ravel.app.middleware import Middleware
from ravel.util.loggers import console
from ravel.ext.falcon.service import FalconService


class ManageCsrfToken(Middleware):
    """
    This middleware expects request.session to be set with a csrf_token
    attribute, e.g. session.csrf_token.
    """
    def __init__(self, signing_key=None, ttl_minutes=None):
        super().__init__()
        if not signing_key:
            signing_key = uuid4().hex
            console.warning(
                message=(
                    'no csrf signing key provided. '
                    'using random default'
                ),
                data={'random_key': signing_key}
            )

        if not isinstance(signing_key, bytes):
            signing_key = signing_key.encode('utf-8')

        self.signing_key = signing_key
        self.ttl_minutes = ttl_minutes

    @property
    def app_types(self) -> Tuple[Type['Application']]:
        return (FalconService, )

    def pre_request(
        self,
        action: 'Action',
        request: 'Request',
        raw_args: Tuple,
        raw_kwargs: Dict
    ):
        # NOTE: set csrf_protected=False in your Action decorators
        # in order to skip this middleware...
        csrf_protected = action.decorator.kwargs.get('csrf_protected')
        if csrf_protected is None:
            csrf_protected = True

        if not csrf_protected:
            return

        http_request = raw_args[0]
        token = http_request.headers.get('CSRF-TOKEN')
        session = request.session

        # bail if token missing or mismatching
        if (not (token and session)) or (session.csrf_token != token):
            raise Exception('CSRF token missing or unrecognized')

        now = datetime.now()
        iv = session.csrf_aes_cbc_iv   # cipher mode initialization vector

        # decode & decrypt the AES encrypted CSRF token
        try:
            aes_cipher = AES.new(self.signing_key, AES.MODE_CBC, iv)
            json_str = unpad(aes_cipher.decrypt(b64decode(token)), AES.block_size)
            token_components = self.app.json.decode(json_str)
            expires_at_isoformat = token_components[0]

            # convert expiration date string to a datetime object
            if expires_at_isoformat is not None:
                expires_at = datetime.fromisoformat(expires_at_isoformat)
            else:
                expires_at = None

        except Exception:
            console.error(
                message='failed to decode CSRF token',
                data={'token': token, 'session_id': session._id}
            )
            raise

        # save crsf data to request for use in post_request
        request.context.csrf = {
            'session_id': token_components[1],
            'expires_at': token_components[0],
            'csrf_token': token,
        }

        # abort request if token is expired (provided it has an expiry)
        if (expires_at is not None) and (now >= expires_at):
            console.error(
                message='received expired CSRF token',
                data={
                    'csrf_token': token,
                    'expires_at': expires_at,
                    'session_id': session._id,
                }
            )
            raise Exception('invalid CSRF token')

        if request.context.csrf['session_id'] != session._id:
            console.error(
                message='CSRF token session ID mismatch',
                data={
                    'csrf_session_id': request.context.csrf['session_id'],
                    'request_session_id': session._id,
                }
            )
            raise Exception('invalid CSRF token')
        
    def post_request(
        self,
        action: 'Action',
        request: 'Request',
        result,
    ):
        """
        Create and set a new token if the current one is expired or
        non-existent.
        """
        http_request, http_response = request.raw_args[:2]
        csrf_data = request.context.get('csrf')
        session = request.session
        regenerate_token = action.decorator.kwargs.get(
            'refresh_csrf_token', False
        )
        if (not csrf_data) or regenerate_token:
            console.debug(f'generating CSRF token for session {session._id}')
            self.create_and_set_token(http_request, http_response, session)

    def create_and_set_token(self, http_request, http_response, session):
        """
        Generate a new CSRF token, using a mix of random values and
        client-specific data.
        """

        # if there is an expiration date for the token, create the
        # datetime ISO format string for it...
        expires_at = None
        if self.ttl_minutes is not None:
            expires_at = (
                datetime.now() + timedelta(minutes=self.ttl_minutes)
            ).isoformat()

        # create the JSON array that we encrypt as the token
        json_token = self.app.json.encode([
            expires_at,
            session._id,
            # sources of entropy:
            http_request.user_agent,
            http_request.access_route[0].lower(),  # client IP
        ]).encode('utf-8')

        # encrypt and B64 encode the token
        iv = get_random_bytes(16)  # AKA "initialization vector"
        aes_cipher = AES.new(self.signing_key, AES.MODE_CBC, iv)
        encypted_token = b64encode(
            aes_cipher.encrypt(pad(json_token, AES.block_size))
        ).decode('utf-8')

        # update the user session's CSRF fields
        session.csrf_token = encypted_token
        session.csrf_aes_cbc_iv = iv

        # send CSRF token back to client
        http_response.set_header('CSRF-TOKEN', encypted_token)
