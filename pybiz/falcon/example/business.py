from pybiz import BizObject
from pybiz.falcon.example.service import api


class User(BizObject):

    @classmethod
    def schema(cls):
        return None

    @api.get('/users/{user_id}')
    def get_user(request, response, user_id):
        return {'id': user_id}


class Account(BizObject):

    @classmethod
    def schema(cls):
        return None

    @api.get('/accounts/{account_id}')
    def get_user(request, response, account_id):
        return {'id': account_id}
