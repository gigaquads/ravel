from typing import Text

import pybiz


class User(pybiz.BizObject):
    email = pybiz.String(required=True, nullable=False)
    password = pybiz.BcryptString(rounds=14, private=True, required=True, nullable=False)

    @staticmethod
    def __abstract__():
        return True
