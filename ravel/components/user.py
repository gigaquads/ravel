import os

from typing import Text

from ravel import Resource, fields

BCRYPT_ROUNDS = int(os.environ.get('USER_PASSWORD_BCRYPT_ROUNDS', 14))


class User(Resource):
    email = fields.String(required=True, nullable=True, default=lambda: None)
    password = fields.BcryptString(
        rounds=BCRYPT_ROUNDS, private=True, required=True, nullable=False
    )
    phone = fields.String(nullable=True)

    @classmethod
    def __abstract__(cls):
        return True
