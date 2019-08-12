import inspect

from typing import Dict, Tuple, Set, Type, List

from appyratus.memoize import memoized_property

from pybiz.util.loggers import console

from .base import ApiMiddleware


class DaoHistoryMiddleware(ApiMiddleware):

    def __init__(self, echo=False, verbose=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dao_instances = []
        self._echo = echo
        self._echo_verbose = verbose

    def on_bootstrap(self):
        for biz_type in self.api.biz.values():
            dao = biz_type.get_dao()
            self._dao_instances.append(dao)
            dao.history.start()

    def pre_request(self, proxy: 'ApiProxy', raw_args: List, raw_kwargs: Dict):
        """
        In pre_request, args and kwargs are in the raw form before being
        processed by api.on_request.
        """
        for dao in self._dao_instances:
            dao.history.clear()

    def on_request(self, proxy: 'ApiProxy', args: List, kwargs: Dict):
        """
        In on_request, args and kwargs are in the form output by
        api.on_request.
        """

    def post_request(
        self, proxy: 'ApiObject', raw_args: List, raw_kwargs: Dict,
        args: List, kwargs: Dict, result, exc: Exception = None
    ):
        """
        In post_request, args and kwargs are in the form output by
        api.on_request.
        """
        if self._echo:
            for dao in self._dao_instances:
                if not self._echo_verbose:
                    console.info(
                        message=f'{dao} history',
                        data={'events': dao.history.events}
                    )
                else:
                    console.info(
                        message=f'{dao} history',
                        data={'events': [e.dump() for e in dao.history.events]}
                    )

