""" Example: dao/basic_creation.py

This file shows you how to implement a Dao class. In this example, we simply use
a plain ol' Python dictionary as our storage backend; however, in reality, this
can be MySQL, PostgreSQL, Mongo, Neo4J, an API sever, etc.

This file also demonstrate one way of specifying dynamically which Dao class a
BizObject class should utilize. Also see the comment in the __dao__ method
below.

Notice in this example how the User BizObject already knows how to call the
appropriate save, read, update, and delete methods on its Dao.
"""

import uuid

from pybiz import BizObject, Dao, fields


class UserDao(Dao):

    users = {}

    def exists(self, _id):
        return _id in self.users

    def fetch(self, _id, fields: dict = None):
        data = self.users.get(_id)
        if data and fields:
            data = {k: data[k] for k in fields}
        return data

    def fetch_many(self, _ids, fields: dict = None):
        return [self.fetch(_id, fields) for _id in _ids]

    def create(self, _id, data):
        self.users[_id] = data
        data['_id'] = _id
        return data

    def update(self, _id, new_data):
        self.users.setdefault(_id, {}).update(new_data)
        return self.fetch(_id)

    def update_many(self, _ids: list, data: list):
        return [self.update(_id, data) for _id in zip(_ids, data)]

    def delete(self, _id):
        return self.users.pop(_id, None)

    def delete_many(self, _ids):
        return [self.users.pop(_id, None) for _id in _ids]


class User(BizObject):

    _id = fields.Int(dump_to='id')
    name = fields.Str()

    @classmethod
    def __dao__(cls):
        # NOTE: The standard way of associating a BizObject class with a Dao
        # class is by declaring it in a pybiz manifest yaml file. Declaring it
        # here is merely intended as a means of overriding the statically
        # defined association, if need be.
        return UserDao



if __name__ == '__main__':

    # create a new User bizobj for the sake of exploring
    # its internal use of the `UserDao`.
    user = User(_id=1, name='Bob')

    print('>>> User {} has these "dirty" fields: {}'.format(
        user.name, set(user.dirty)))

    print('>>> Calling save...')
    user.save()

    print('>>> Now the dirty set is empty: {}'.format(
        set(user.dirty)))

    print('>>> The user data store now looks like: {}'.format(
        user.dao.users))

    print('>>> Editing Bob\'s name...')
    user.name = 'Bobbert'

    print('>>> Calling save...')
    user.save()

    print('>>> The user data store now looks like: {}'.format(
        user.dao.users))

    print('>>> Calling delete...')
    user.delete()

    print('>>> The user data store now looks like: {}'.format(
        user.dao.users))

