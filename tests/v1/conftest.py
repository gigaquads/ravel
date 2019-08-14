import uuid
import pytest
import pybiz

from pybiz.test.domains.startrek import startrek as startrek_app


@pytest.fixture(scope='session')
def startrek_manfiest():
    return {
        'package': 'pybiz.test.domains.startrek',
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
    return startrek.biz.Officer.BizList([
        captain_picard,
        lieutenant_worf,
    ])


@pytest.fixture(scope='function')
def the_enterprise(startrek):
    return startrek.biz.Ship(name='Enterprise')


@pytest.fixture(scope='function')
def the_enterprise_with_crew(the_enterprise, enterprise_crew):
    the_enterprise._id = uuid.uuid4()
    enterprise_crew.merge(ship_id=the_enterprise._id)
    the_enterprise.crew = enterprise_crew
    return the_enterprise
