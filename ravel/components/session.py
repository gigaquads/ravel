from typing import Type

from appyratus.utils import TimeUtils

from ravel import Resource, Relationship, fields
from ravel.query.predicate import Predicate

from .user import User


class Session(Resource):
    is_active = fields.Bool(nullable=False, default=True)
    logged_out_at = fields.DateTime(nullable=True)
    owner_id = fields.Field(required=True, nullable=True, private=True)
    owner = Relationship(
        join=lambda self: (Session.owner_id, self.get_user_type()._id)
    )

    @classmethod
    def __abstract__(cls):
        return True

    @classmethod
    def get_user_type(cls) -> Type[User]:
        user_type = cls.app.res.get('User')
        if user_type is None:
            raise NotImplementedError('return a User subclass')
        return user_type

    def logout(self):
        if self.is_active:
            self.update(
                logged_out_at=TimeUtils.utc_now(),
                is_active=False,
            )
