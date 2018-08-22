import uuid

from pybiz import BizObject

from .schemas import UserSchema


class User(BizObject):

    @classmethod
    def __schema__(cls):
        return UserSchema
