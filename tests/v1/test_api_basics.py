import pytest
import pybiz

from appyratus.test import mark


class TestApiBasics(object):
    """
    TODO:
    """

    @mark.integration
    def test_argument_is_loaded_from_id(cls, startrek, captain_picard):
        captain_picard.create()
        get_officer = startrek.proxies['get_officer']
        get_officer(captain_picard._id)
