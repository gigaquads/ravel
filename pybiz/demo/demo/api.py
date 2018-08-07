import os

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
    user = User(name=name, email=email)
    return user.save()


@repl()
@http(http_method='GET', url_path='/users/{public_id}')
def get_user(public_id, *args, **kwargs):
    user = User.get(public_id=public_id)
    return user
