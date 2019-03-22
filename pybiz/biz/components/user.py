from typing import Text

from pybiz import BizObject, fields


class User(BizObject):
    email = fields.Email(required=True, nullable=False)
    password = fields.BcryptString(rounds=14, private=True, required=True, nullable=False)

    @staticmethod
    def __abstract__():
        return True
