import shutil

from ravel.test.crud import *
from ravel.store import SimulationStore, FilesystemStore


class TestResourceCrudWithSimulationStore(ResourceCrudTestSuite):
    @classmethod
    def build_store(cls, app):
        SimulationStore.bootstrap(app)
        return SimulationStore()


class TestResourceCrudWithFilesystemStore(ResourceCrudTestSuite):
    @classmethod
    def build_store(cls, app):
        root_dir = '/tmp/ravel-filesystem-store-crud-tests'

        try:
            shutil.rmtree(root_dir, ignore_errors=True)
        except FileNotFoundError:
            pass
        except OSError as err:
            if err.errno != 66:  # directory not empty
                raise

        FilesystemStore.bootstrap(app, root=root_dir)
        return FilesystemStore()


#class TestResourceCrudWithCacheStore(ResourceCrudTestSuite):
#    pass
#
#
#class TestResourceCrudWithRedisStore(ResourceCrudTestSuite):
#    pass
#
#
#class TestResourceCrudWithSqlalchemyStore(ResourceCrudTestSuite):
#    pass
