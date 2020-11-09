from hashlib import sha256
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
    def __init__(self, signing_key=None, ttl_minutes=24 * 60):
        super().__init__()
        if not signing_key:
            signing_key = uuid4().hex
            console.warning(
                message=f'no csrf signing key provided. using random default',
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
        if not action.decorator.kwargs.get('csrf_protected', True):
            return

        http_request = raw_args[0]
        token = http_request.headers.get('CSRF-TOKEN')
        session = request.session

        if (not (token and session)) or (session.csrf_token != token):
            raise Exception('CSRF token missing or unrecognized')

        now = datetime.now()
        iv = session.csrf_aes_cbc_iv

        try:
            aes_cipher = AES.new(self.signing_key, AES.MODE_CBC, iv)
            json_str = unpad(aes_cipher.decrypt(b64decode(token)), AES.block_size)
            token_components = self.app.json.decode(json_str)
            expires_at = datetime.fromisoformat(token_components[0])
        except Exception:
            console.error(
                message='failed to decode CSRF token',
                data={'token': token, 'session_id': session._id}
            )
            raise

        request.context.csrf = {
            'session_id': token_components[2],
            'expires_at': token_components[0],
            'is_expired': False,
        }

        if now >= expires_at:
            request.context.csrf['is_expired'] = True
            console.info(
                message='received an expired CSRF token',
                data={
                    'csrf_token': token,
                    'expires_at': expires_at,
                    'session_id': session._id,
                }
            )
        
    def post_request(
        self,
        action: 'Action',
        request: 'Request',
        result,
    ):
        http_request, http_response = request.raw_args[:2]
        csrf_data = request.context.get('csrf')
        session = request.session
        if (not csrf_data) or csrf_data['is_expired']:
            self.create_and_set_token(http_request, http_response, session)

    def create_and_set_token(self, http_request, http_response, session):
        """
        Generate a new CSRF token, using a mix of random values and
        client-specific data.
        """
        ip_addr = http_request.access_route[0].lower()
        cannonical_str = self.app.json.encode([
            (datetime.now() + timedelta(self.ttl_minutes)).isoformat(),
            uuid4().hex,
            session._id,
            http_request.user_agent,
            ip_addr,
        ]).encode('utf-8')

        iv = get_random_bytes(16)
        aes_cipher = AES.new(self.signing_key, AES.MODE_CBC, iv)

        new_token = b64encode(
            aes_cipher.encrypt(pad(cannonical_str, AES.block_size))
        ).decode('utf-8')

        session.csrf_token = new_token
        session.csrf_aes_cbc_iv = iv

        http_response.set_header('CSRF-TOKEN', new_token)
