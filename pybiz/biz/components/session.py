from typing import Type

from pybiz import BizObject, Relationship, fields
from pybiz.predicate import Predicate

from .user import User


class Session(BizObject):
    user_id = fields.Field(required=True, nullable=True, private=True)
    user = Relationship(
        conditions=(
            lambda rel, self: (
                rel.biz_type.user_type(), self.build_user_predicate())
        ),
        readonly=True,
    )

    @staticmethod
    def __abstract__():
        return True

    @classmethod
    def user_type(cls) -> Type[User]:
        user_type = cls.registry.types.biz.get('User')
        if user_type is None:
            raise NotImplementedError('return a User subclass')
        return user_type

    def build_user_predicate(self) -> Predicate:
        User = self.user_type()
        if self.user_id is not None:
            return User._id == self.user_id
