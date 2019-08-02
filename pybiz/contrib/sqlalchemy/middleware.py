from typing import List, Dict, Text, Type, Set, Tuple

from pybiz.api.middleware import ApiMiddleware

from .dao import SqlalchemyDao


class SqlalchemyMiddleware(ApiMiddleware):
    def on_bootstrap(self):
        self.SqlalchemyDao = self.api.types.dao['SqlalchemyDao']

    def pre_request(self, proxy, raw_args: Tuple, raw_kwargs: Dict):
        """
        In pre_request, args and kwargs are in the raw form before being
        processed by api.on_request.
        """
        self.SqlalchemyDao.connect()
        self.SqlalchemyDao.begin()

    def post_request(
        self, proxy: 'ApiObject', raw_args: Tuple, raw_kwargs: Dict,
        args: Tuple, kwargs: Dict, result, exc: Exception = None
    ):
        """
        In post_request, args and kwargs are in the form output by
        api.on_request.
        """
        # TODO: pass in exc to post_request if there
        #   was an exception and rollback
        try:
            self.SqlalchemyDao.commit()
        except:
            self.SqlalchemyDao.rollback()
        finally:
            self.SqlalchemyDao.close()
