import os

from .service import api_factory
from .biz import User


api = api_factory()


@api(http_method='GET', url_path='/status')
def echo(echo=None, *args, **kwargs):
    return {'echo': echo}


@api(http_method='POST', url_path='/users')
def create_user(name, email=None, *args, **kwargs):
    user = User(name=name, email=email)
    return user.save()


@api(http_method='GET', url_path='/users/{public_id}')
def get_user(public_id, *args, **kwargs):
    user = User.get(public_id=public_id)
    return user
