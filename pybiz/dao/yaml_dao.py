import os

from appyratus.io import File, Yaml

from pybiz.dao.dict_dao import DictDao


class YamlDao(DictDao):
    @classmethod
    def read_all(cls):
        profile_data = []
        for root, dirs, files in os.walk(self.data_path):
            for yamlfile in files:
                filepath = os.path.join(root, yamlfile)
                with open(filepath) as yamlraw:
                    res = yaml.load(yamlraw.read())
                    profile_data.append(res)
        return profile_data

    @classmethod
    def file_path(cls, key):
        return os.path.join(cls.data_path, Yaml.format_file_name(key))

    def exists(self, _id=None, public_id=None) -> bool:
        raise NotImplementedError('override `exists` in subclass')

    def fetch(self, _id=None, public_id=None, fields: dict=None) -> dict:
        data = super().fetch(_id=_id, public_id=public_id, fields=fields)
        if not data:
            file_path = self.file_path(public_id or _id)
            if not File.exists(file_path):
                    raise Exception('File does not exist, {}'.format(file_path))
            data = Yaml.from_file(file_path)
        return data

    def fetch_many(
        self, _ids: list=None, public_ids: list=None, fields: dict=None
    ) -> dict:
        raise NotImplementedError('override in subclass')

    def create(self, _id=None, public_id=None, data: dict=None) -> dict:
        data = super().create(_id=_id, public_id=public_id, data=data)
        file_path = self.file_path(public_id or _id)
        if File.exists(file_path):
            raise Exception('File exists at {}'.format(file_path))
        Yaml.to_file(file_path=file_path, data=data)
        return data

    def update(self, _id=None, public_id=None, data: dict=None) -> dict:
        file_path = self.file_path(public_id or _id)
        if not File.exists(file_path):
            raise Exception('File does not exist at {}'.format(file_path))
        data = super().update(_id=_id, public_id=public_id, data=data)
        pass

    def update_many(
        self, _ids: list=None, public_ids: list=None, data: list=None
    ) -> dict:
        raise NotImplementedError('override in subclass')

    def delete(self, _id=None, public_id=None) -> dict:
        raise NotImplementedError('override in subclass')

    def delete_many(self, _ids: list=None, public_ids: list=None) -> dict:
        raise NotImplementedError('override in subclass')
