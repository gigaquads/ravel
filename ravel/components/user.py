import os

from typing import Text

import ravel


class User(ravel.Resource):

    @classmethod
    def __abstract__(cls):
        return True

    email = ravel.String(required=True, nullable=False)
    password = ravel.BcryptString(
        rounds=int(os.environ.get('USER_PASSWORD_BCRYPT_ROUNDS', 14)),
        private=True, required=True, nullable=False
    )
