from typing import Type

from pybiz import BizObject, Relationship, fields
from pybiz.predicate import Predicate

from .user import User


class Session(BizObject):
    user_id = fields.Field(required=True, nullable=True, private=True)
    user = Relationship(
        conditions=lambda self: (self._user_type, self.resolve_user()),
        readonly=True,
    )

    @staticmethod
    def __abstract__():
        return True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._user_type = self.user_type()

    @staticmethod
    def user_type() -> Type[User]:
        raise NotImplementedError('return a User subclass')

    def resolve_user(self) -> Predicate:
        if self.user_id is not None:
            return self._user_type._id == self.user_id
