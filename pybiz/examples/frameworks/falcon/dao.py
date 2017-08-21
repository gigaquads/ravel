from pybiz import Dao


class UserDao(Dao):

    users = {}

    def exists(self, _id=None, public_id=None):
        return _id in self.users

    def fetch(self, _id=None, public_id=None, fields: dict=None):
        data = self.users.get(_id)
        if data and fields:
            data = {k: data[k] for k in fields}
        return data

    def fetch_many(self, _ids=None, public_ids=None, fields: dict=None):
        return [self.fetch(_id, fields) for _id in _ids]

    def create(self, _id=None, public_id=None, data=None):
        if _id is not None:
            self.users[_id] = data
        if public_id is not None:
            self.users[public_id] = data
        data['_id'] = _id
        data['public_id'] = public_id
        return data

    def update(self, _id=None, public_id=None, new_data=None):
        self.users.setdefault(_id, {}).update(new_data)
        return self.fetch(_id)

    def update_many(self, _ids=None, public_ids=None, data: list=None):
        return [self.update(_id, data) for _id in zip(_ids, data)]

    def delete(self, _id=None, public_id=None):
        return self.users.pop(_id, None)

    def delete_many(self, _ids=None, public_ids=None):
        deleted_data = [self.users.pop(_id, None) for _id in _ids]
