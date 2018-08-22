import os

from appyratus.validation.fields import Uuid

from .service import DemoFalconService, DemoRepl
from .biz import User


http = DemoFalconService()
repl = DemoRepl()


@http(http_method='GET', url_path='/status')
def echo(echo=None, *args, **kwargs):
    return {'echo': echo}


@repl()
@http(http_method='POST', url_path='/users')
def create_user(name, email=None, *args, **kwargs):
    user_id = Uuid.next_uuid()
    user = User(_id=user_id, name=name, email=email)
    return user.save()


@repl()
@http(http_method='GET', url_path='/users/{user_id}')
def get_user(user_id, *args, **kwargs):
    user = User.get(_id=user_id)
    return user
