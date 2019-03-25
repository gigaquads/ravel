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

    @staticmethod
    def user_type() -> Type[User]:
        raise NotImplementedError('return a User subclass')

    def build_user_predicate(self) -> Predicate:
        User = self.user_type()
        if self.user_id is not None:
            return User._id == self.user_id
