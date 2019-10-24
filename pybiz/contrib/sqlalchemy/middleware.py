from typing import List, Dict, Text, Type, Set, Tuple

from pybiz.app.middleware import ApplicationMiddleware
from pybiz.util.loggers import console

from .dao import SqlalchemyDao


class SqlalchemyMiddleware(ApplicationMiddleware):
    def on_bootstrap(self):
        self.SqlalchemyDao = self.app.types.dal['SqlalchemyDao']

    def pre_request(self, endpoint, raw_args: Tuple, raw_kwargs: Dict):
        """
        In pre_request, args and kwargs are in the raw form before being
        processed by app.on_request.
        """
        self.SqlalchemyDao.connect()
        self.SqlalchemyDao.begin()

    def post_request(
        self,
        endpoint: 'Endpoint',
        raw_args: Tuple,
        raw_kwargs: Dict,
        args: Tuple,
        kwargs: Dict,
        result,
        exc: Exception = None,
    ):
        """
        In post_request, args and kwargs are in the form output by
        app.on_request.
        """
        # TODO: pass in exc to post_request if there
        #   was an exception and rollback
        try:
            if exc is not None:
                raise exc
            console.debug(
                f'{self.__class__.__name__} trying to commit transaction'
            )
            self.SqlalchemyDao.commit()
        except:
            console.error(
                f'{self.__class__.__name__} rolling back transaction'
            )
            self.SqlalchemyDao.rollback()
        finally:
            self.SqlalchemyDao.close()
