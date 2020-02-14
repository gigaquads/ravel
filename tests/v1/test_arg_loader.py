import pytest
import pybiz

from appyratus.test import mark


class TestApplicationBasics(object):
    """
    TODO:
    """

    @mark.integration
    def test_positional_arg_is_loaded_from_id(cls, startrek, captain_picard):
        captain_picard.create()
        args, kwargs = startrek.loader.load(
            endpoint=startrek.api.get_officer,
            args=(captain_picard._id, ),
            kwargs={}
        )
        assert isinstance(args[0], startrek.biz.Officer)
        assert args[0]._id == captain_picard._id

    @mark.integration
    def test_positional_arg_is_loaded_from_dict(cls, startrek, captain_picard):
        captain_picard.create()
        args, kwargs = startrek.loader.load(
            endpoint=startrek.api.get_officer,
            args=(captain_picard.dump(), ),
            kwargs={}
        )
        assert isinstance(args[0], startrek.biz.Officer)
        assert args[0]._id == captain_picard._id

    @mark.integration
    def test_positional_arg_is_loaded_from_resource(cls, startrek, captain_picard):
        captain_picard.create()
        args, kwargs = startrek.loader.load(
            endpoint=startrek.api.get_officer,
            args=(captain_picard, ),
            kwargs={}
        )
        assert isinstance(args[0], startrek.biz.Officer)
        assert args[0]._id == captain_picard._id

    @mark.integration
    def test_kw_arg_is_loaded_from_id(cls, startrek, the_enterprise):
        the_enterprise.create()
        args, kwargs = startrek.loader.load(
            endpoint=startrek.api.get_ship,
            args=tuple(),
            kwargs={'ship': the_enterprise._id}
        )
        assert isinstance(kwargs['ship'], startrek.biz.Ship)
        assert kwargs['ship']._id == the_enterprise._id

    @mark.integration
    def test_kw_arg_is_loaded_from_dict(cls, startrek, the_enterprise):
        the_enterprise.create()
        args, kwargs = startrek.loader.load(
            endpoint=startrek.api.get_ship,
            args=tuple(),
            kwargs={'ship': the_enterprise.dump()}
        )
        assert isinstance(kwargs['ship'], startrek.biz.Ship)
        assert kwargs['ship']._id == the_enterprise._id

    @mark.integration
    def test_kw_arg_is_loaded_from_resource(cls, startrek, the_enterprise):
        the_enterprise.create()
        args, kwargs = startrek.loader.load(
            endpoint=startrek.api.get_ship,
            args=tuple(),
            kwargs={'ship': the_enterprise}
        )
        assert isinstance(kwargs['ship'], startrek.biz.Ship)
        assert kwargs['ship']._id == the_enterprise._id

    @mark.integration
    def test_kw_arg_is_nullified_if_arg_not_resolved(cls, startrek):
        args, kwargs = startrek.loader.load(
            endpoint=startrek.api.get_officer,
            args=tuple('invalid-id'),
            kwargs={}
        )
        assert args[0] is None

    @mark.integration
    def test_kw_arg_is_nullified_if_kwarg_not_resolved(cls, startrek):
        args, kwargs = startrek.loader.load(
            endpoint=startrek.api.get_ship,
            args=tuple(),
            kwargs={'ship': 'invalid-id'}
        )
        assert kwargs['ship'] is None
