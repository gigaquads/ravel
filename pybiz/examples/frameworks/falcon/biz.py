import uuid

from pybiz import BizObject

from .service import api
from .schemas import UserSchema, CreateUserSchema


class User(BizObject):

    @classmethod
    def __schema__(cls):
        return UserSchema

    @api.get('/users/{user_id}', schemas={
        'response': UserSchema()
        })
    def get_user(user_id):
        return User.get(_id=user_id)

    @api.post('/users', schemas={
        'request': CreateUserSchema()
        })
    def create_user(name, email):
        user = User(name=name, email=email).save()
        return user
