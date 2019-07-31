from typing import Type

from pybiz import BizObject, Relationship, fields
from pybiz.predicate import Predicate

from .user import User


class Session(BizObject):
    user_id = fields.Field(required=True, nullable=True, private=True)
    user = Relationship(lambda self: (Session.user_id, self.user_type()._id))

    @staticmethod
    def __abstract__():
        return True

    @classmethod
    def user_type(cls) -> Type[User]:
        user_type = cls.registry.types.biz.get('User')
        if user_type is None:
            raise NotImplementedError('return a User subclass')
        return user_type
