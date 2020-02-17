import pytest
import ravel

from appyratus.schema.fields import Uuid
from ravel.test.domains.startrek import startrek as startrek_app


@pytest.fixture(scope='session')
def startrek_manfiest():
    return {
        'package': 'ravel.test.domains.startrek',
    }


@pytest.fixture(scope='function')
def startrek(startrek_manfiest):
    return startrek_app.bootstrap(
        manifest=startrek_manfiest,
        rebootstrap=True
    )


@pytest.fixture(scope='function')
def captain_picard(startrek):
    return startrek.biz.Officer(first_name='Picard', rank='captain')


@pytest.fixture(scope='function')
def lieutenant_worf(startrek):
    return startrek.biz.Officer(first_name='Worf', rank='lieutenant')


@pytest.fixture(scope='function')
def random_recruit(startrek):
    return startrek.biz.Officer.generate()


@pytest.fixture(scope='function')
def enterprise_crew(startrek, the_enterprise, captain_picard, lieutenant_worf):
    return startrek.biz.Officer.Batch([
        captain_picard,
        lieutenant_worf,
    ])


@pytest.fixture(scope='function')
def the_enterprise(startrek):
    return startrek.biz.Ship(name='Enterprise')


@pytest.fixture(scope='function')
def the_enterprise_with_crew(the_enterprise, enterprise_crew):
    the_enterprise._id = Uuid.next_id()
    enterprise_crew.merge(ship_id=the_enterprise._id)
    the_enterprise.crew = enterprise_crew
    return the_enterprise


@pytest.fixture(scope='function')
def missions(startrek):
    Mission = startrek.biz.Mission
    return Mission.Batch(
        [
            Mission(name="Defeat the borg", description="Infiltrate unimatrix 01"),
            Mission(name="Escape Delta Quadrant", description="Steal transwarp drive"),
        ]
    )
