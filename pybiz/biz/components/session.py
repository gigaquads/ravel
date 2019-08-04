from typing import Type

from appyratus.utils import TimeUtils

from pybiz import BizObject, Relationship, fields
from pybiz.predicate import Predicate

from .user import User


class Session(BizObject):
    is_active = fields.Bool(nullable=False, default=True)
    logged_out_at = fields.DateTime(nullable=True)
    user_id = fields.Field(required=True, nullable=True, private=True)
    user = Relationship(lambda self: (Session.user_id, self.user_type()._id))

    @staticmethod
    def __abstract__():
        return True

    @classmethod
    def user_type(cls) -> Type[User]:
        user_type = cls.api.types.biz.get('User')
        if user_type is None:
            raise NotImplementedError('return a User subclass')
        return user_type


    def logout(self):
        if self.is_active:
            self.update(
                logged_out_at=TimeUtils.utc_now(),
                is_active=False,
            )
