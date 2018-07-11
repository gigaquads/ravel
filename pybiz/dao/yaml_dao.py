import os

from appyratus.types import Yaml, File
from appyratus.util import TextTransform

from pybiz.dao import Dao


class YamlDao(Dao):
    """
    # Yaml DAO
    This is actually a File system Dao, with a yaml interpreter applied to the
    data as it passes in and out.  That is to say this could one day be
    abstracted into such a `FileSystemDao`, and be inherited here.
    """

    @classmethod
    def data_path(cls) -> str:
        """
        # Data path
        Absolute path to the yaml data files associated with this DAO
        """
        dao_name = TextTransform.snake(cls.__name__.replace('Dao', ''))
        dao_path = 'data/{}'.format(dao_name)
        return os.path.abspath(dao_path)

    @classmethod
    def file_path(cls, key) -> str:
        """
        # File path
        Path to a specific data file

        ## Args
        - `key`: they file name less extension
        """
        return os.path.join(cls.data_path(), Yaml.format_file_name(key))

    def exists(self, _id=None, public_id=None) -> bool:
        return File.exists(self.file_path(public_id or _id))

    def fetch(self, _id=None, public_id=None, fields: dict=None) -> dict:
        data = super().fetch(_id=_id, public_id=public_id, fields=fields)
        if not data:
            file_path = self.file_path(public_id or _id)
            if not self.exists(public_id=public_id, _id=_id):
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
        if self.exists(public_id=public_id, _id=_id):
            raise Exception('File exists at {}'.format(file_path))
        Yaml.to_file(file_path=file_path, data=data)
        if not _id and '_id' not in data:
            data['_id'] = public_id
        return data

    def update(self, _id=None, public_id=None, data: dict=None) -> dict:
        file_path = self.file_path(public_id or _id)
        if not self.exists(public_id=public_id, _id=_id):
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

    @classmethod
    def read_all(cls):
        """
        Read all yaml files in data path
        XXX This may not be in use
        """
        profile_data = []
        for root, dirs, files in os.walk(cls.data_path()):
            for yamlfile in files:
                filepath = os.path.join(root, yamlfile)
                with open(filepath) as yamlraw:
                    res = yaml.load(yamlraw.read())
                    profile_data.append(res)
        return profile_data
