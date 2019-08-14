import inspect

from typing import Dict, Tuple, Set, Type, List

from appyratus.memoize import memoized_property

from pybiz.util.loggers import console

from .application_middleware import ApplicationMiddleware


class DaoHistoryMiddleware(ApplicationMiddleware):

    def __init__(self, echo=False, verbose=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dao_instances = []
        self._echo = echo
        self._echo_verbose = verbose

    def on_bootstrap(self):
        for biz_type in self.app.biz.values():
            dao = biz_type.get_dao()
            self._dao_instances.append(dao)
            dao.history.start()

    def pre_request(self, endpoint: 'Endpoint', raw_args: List, raw_kwargs: Dict):
        """
        In pre_request, args and kwargs are in the raw form before being
        processed by app.on_request.
        """
        for dao in self._dao_instances:
            dao.history.clear()

    def on_request(self, endpoint: 'Endpoint', args: List, kwargs: Dict):
        """
        In on_request, args and kwargs are in the form output by
        app.on_request.
        """

    def post_request(
        self, endpoint: 'ApplicationObject', raw_args: List, raw_kwargs: Dict,
        args: List, kwargs: Dict, result, exc: Exception = None
    ):
        """
        In post_request, args and kwargs are in the form output by
        app.on_request.
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
