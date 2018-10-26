import os

from appyratus.types import Yaml, File
from appyratus.util import TextTransform, DictUtils

from pybiz.dao import Dao


class YamlDao(Dao):
    """
    # Yaml DAO
    This DAO simply reads and write a single YAML file stored in a data
    directory read from the YAML_DAO_DATA_DIR environment variable.
    """
    def __init__(self, id_key=None, *args, **kwargs):
        self._id_key = id_key or '_id'
        super().__init__(*args, **kwargs)

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

    def create(self, data: dict) -> dict:
        """
        Create the YAML file.
        """
        _id = data[self._id_key]
        assert _id is not None
        file_path = self.file_path(_id)
        if self.exists(_id):
            raise Exception('File exists at {}'.format(file_path))
        data['_id'] = _id
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
        # get a copy of the current yaml data and then merge it with what is
        # being update.  this is important 
        cur_data = self.fetch(_id)
        new_data = DictUtils.merge(cur_data, data)
        Yaml.to_file(file_path=file_path, data=new_data)
        return new_data

    def delete(self, _id) -> None:
        """
        Delete the YAML file.
        """
        os.unlink(_id)

    def create_many(self, records: list) -> list:
        raise NotImplementedError('not implemented')

    def fetch_many(self, _ids: list, fields: dict = None) -> dict:
        if not _ids:
            return {}
        records = {}
        for _id in _ids:
            record = self.fetch(_id, fields=fields)
            if record:
                records[_id] = record
        return records

    def update_many(self, _ids: list, updates: list = None) -> dict:
        raise NotImplementedError('does not make sense to implement2')

    def delete_many(self, _ids: list) -> dict:
        raise NotImplementedError('does not make sense to implement3')

    def query(self, predicate, **kwargs):
        raise NotImplementedError('')
