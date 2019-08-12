import pytest
import pybiz


@pytest.fixture(scope='function')
def startrek():
    api = pybiz.Api()

    @api()
    def get_officer(officer: 'Officer') -> 'Person':
        return officer

    api.bootstrap({'package': 'pybiz.test.domains.startrek'})

    for biz_type in api.biz.to_dict().values():
        biz_type.get_dao().delete_all()

    return api


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
