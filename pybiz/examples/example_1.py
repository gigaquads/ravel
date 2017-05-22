import random
import uuid

from pybiz.schema import Schema, Str, Int, List, Nested
from pybiz.dao import Dao, DaoManager
from pybiz.patch import pre_patch, post_patch
from pybiz.biz import BizObject
from pybiz import api


class UserMockDao(Dao):

    def fetch(_id, fields: dict = None):
        return {}

    def fetch_many(_ids, fields: dict = None):
        pass

    def create(self, data):
        data['_id'] = random.randint(1, 1000)
        return data

    def save(self, data, _id=None):
        print('Saving {}'.format(data))
        return 1 if not _id else data.get('_id', 1)

    def save_many(self, id_data_pairs: list):
        pass

    def delete(self, _id):
        pass

    def delete_many(self, _ids):
        pass


class UserSchema(Schema):
    class NameSchema(Schema):
        first = Str(required=True, allow_none=True)
        last = Str(required=False)

    _id = Int(allow_none=True)
    public_id = Str(allow_none=True)
    name = Nested(NameSchema(), allow_none=True)
    numbers = List(Int())
    email = Str()
    age = Int()


class User(BizObject):

    @classmethod
    def schema(cls):
        return UserSchema

    @api.post('/users')
    def create_user_api(request, response):
        user = User.create({
            '_id': None,
            'public_id': uuid.uuid4().hex,
            'name': {'first': 'Leonardo', 'last': 'DaVinci'},
            'email': 'davinci666@gmail.com',
            'numbers': [1, 2, 3],
            'age': 967,
            })
        user.save()
        return user

    @classmethod
    def create(cls, data, **kwargs_data):
        data.update(kwargs_data)
        created_data = User.get_dao().create(data)
        user = cls(created_data)
        return user

    @pre_patch('/name')
    def pre_patch_name(self, op, path, value):
        print('About to patch name!')

    @pre_patch('/name/first')
    def pre_patch_first_name(self, op, path, value):
        print('About to patch first name!')

    @post_patch('/name/first')
    def post_patch_first_name(self, op, path, value):
        print('Patched first name!')


if __name__ == '__main__':
    import json

    def pp(obj):
        print(json.dumps(obj, sort_keys=True, indent=2))

    DaoManager.get_instance().register(
        'mock', {
            'User': UserMockDao,
            })

    user = User(
            _id=None,
            public_id=uuid.uuid4().hex,
            name={'first': 'Leonardo', 'last': 'DaVinci'},
            email='davinci666@gmail.com',
            numbers=[1, 2, 3],
            age=967)

    user.save()
    pp(user.dump())

    # Test JsonPatch integration
    user.patch('replace', '/name/first', 'Leo')
    user.save()
    pp(user.dump())

    user.patch('remove', '/name/first')
    user.save()
    pp(user.dump())

    user.patch('remove', '/numbers/1')
    user.patch('add', '/numbers', 4)
    user.save()
    pp(user.dump())

    user.patch('replace', '/name', {'first': 'Don'})
    user.save()
    pp(user.dump())

    user.patch('remove', '/name')
    user.save()
    pp(user.dump())
    print(user)

    # now test REST API integration:
    api.registry.route('POST', '/users', (None, None))
