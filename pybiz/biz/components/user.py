import bcrypt

from typing import Text

from pybiz import BizObject, fields


class User(BizObject):
    email = fields.Email(required=True, nullable=False)
    password = fields.String(
        private=True, nullable=True, default=lambda: None, required=True
    )

    @staticmethod
    def __abstract__():
        return True

    def bcrypt_set_password(self, raw_password: Text, rounds=14):
        self.password = bcrypt.hashpw(raw_password, bcrypt.gensalt(rounds))

    def bcrypt_check_password(self, raw_password: Text) -> bool:
        return bcrypt.checkpw(raw_password, self.password)
