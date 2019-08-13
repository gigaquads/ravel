from typing import List, Dict, Text, Type, Set, Tuple

from pybiz.app.middleware import ApplicationMiddleware

from .dao import SqlalchemyDao


class SqlalchemyMiddleware(ApplicationMiddleware):
    def on_bootstrap(self):
        self.SqlalchemyDao = self.app.types.dao['SqlalchemyDao']

    def pre_request(self, endpoint, raw_args: Tuple, raw_kwargs: Dict):
        """
        In pre_request, args and kwargs are in the raw form before being
        processed by app.on_request.
        """
        self.SqlalchemyDao.connect()
        self.SqlalchemyDao.begin()

    def post_request(
        self, endpoint: 'ApplicationObject', raw_args: Tuple, raw_kwargs: Dict,
        args: Tuple, kwargs: Dict, result, exc: Exception = None
    ):
        """
        In post_request, args and kwargs are in the form output by
        app.on_request.
        """
        # TODO: pass in exc to post_request if there
        #   was an exception and rollback
        try:
            self.SqlalchemyDao.commit()
        except:
            self.SqlalchemyDao.rollback()
        finally:
            self.SqlalchemyDao.close()
