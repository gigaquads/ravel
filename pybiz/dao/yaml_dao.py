import os

from appyratus.types import Yaml, File
from appyratus.util import TextTransform

from pybiz.dao import Dao


class YamlDao(Dao):
    """
    # Yaml DAO
    This DAO simply reads and write a single YAML file stored in a data
    directory read from the YAML_DAO_DATA_DIR environment variable.
    """

    @classmethod
    def data_path(cls) -> str:
        """
        Absolute path to the yaml data files associated with this DAO.
        """
        base_dir = os.environ.get('YAML_DAO_DATA_DIR', './data')
        dao_name = TextTransform.snake(cls.__name__.replace('Dao', ''))
        dao_path = '{}/{}'.format(base_dir.rstrip('/'), dao_name)
        return os.path.abspath(dao_path)

    @classmethod
    def file_path(cls, key) -> str:
        """
        Path to a specific data file.

        ## Args:
            - `key`: they file name less extension
        """
        return os.path.join(cls.data_path(), Yaml.format_file_name(key))

    @classmethod
    def read_all(cls):  # XXX: Remove this?
        """
        Read all yaml files in data path
        XXX This may not be in use
        """
        profile_data = []
        for root, dirs, files in os.walk(cls.data_path()):
            for yaml_file in files:
                filepath = os.path.join(root, yaml_file)
                with open(filepath) as yamlraw:
                    res = yaml.load(yamlraw.read())
                    profile_data.append(res)
        return profile_data

    def exists(self, _id) -> bool:
        return File.exists(self.file_path(_id))

    def fetch(self, _id=None, fields: dict = None) -> dict:
        """
        Read the YAML file into a dict.
        """
        file_path = self.file_path(_id)
        if not self.exists(_id):
            raise Exception('File does not exist, {}'.format(file_path))
        data = Yaml.from_file(file_path)
        return data

    def create(self, _id, data: dict) -> dict:
        """
        Create the YAML file.
        """
        file_path = self.file_path(_id)
        if self.exists(_id):
            raise Exception('File exists at {}'.format(file_path))
        if _id and '_id' not in data:
            data['_id'] = _id
        assert _id in data
        Yaml.to_file(file_path=file_path, data=data)
        return data

    def update(self, _id, data: dict) -> dict:
        """
        Overwrite the YAML file.
        """
        file_path = self.file_path(_id)
        if not self.exists(_id):
            raise Exception('File does not exist, {}'.format(file_path))
        if '_id' not in data:
            data['_id'] = _id
        Yaml.to_file(file_path=file_path, data=data)
        return data

    def delete(self, _id) -> None:
        """
        Delete the YAML file.
        """
        os.unlink(_id)

    def fetch_many(self, _ids: list, fields: dict = None) -> dict:
        raise NotImplementedError('does not make sense to implement')

    def update_many(self, _ids: list, updates: list = None) -> dict:
        raise NotImplementedError('does not make sense to implement')

    def delete_many(self, _ids: list) -> dict:
        raise NotImplementedError('does not make sense to implement')
