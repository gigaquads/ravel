import os

from typing import Text

import ravel

BCRYPT_ROUNDS = int(os.environ.get('USER_PASSWORD_BCRYPT_ROUNDS', 14))


class User(ravel.Resource):
    email = ravel.String(required=True, nullable=True, default=lambda: None)
    password = ravel.BcryptString(rounds=BCRYPT_ROUNDS, private=True, required=True, nullable=False)
    phone = ravel.String(nullable=True)

    @classmethod
    def __abstract__(cls):
        return True
