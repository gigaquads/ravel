from ravel.test.crud import *
from ravel.store import SimulationStore


class TestResourceCrudForSimulationStore(ResourceCrudTestSuite):

    @classmethod
    def build_store(cls, app):
        SimulationStore.bootstrap(app)
        return SimulationStore()
