import os

from typing import Text

import pybiz


class User(pybiz.Resource):

    @classmethod
    def __abstract__(cls):
        return True

    email = pybiz.String(required=True, nullable=False)
    password = pybiz.BcryptString(
        rounds=int(os.environ.get('USER_PASSWORD_BCRYPT_ROUNDS', 14)),
        private=True, required=True, nullable=False
    )
