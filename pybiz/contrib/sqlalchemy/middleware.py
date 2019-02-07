from typing import List, Dict, Text, Type, Set, Tuple

from pybiz.api.middleware import RegistryMiddleware

from .dao import SqlalchemyDao


class SqlalchemyMiddleware(RegistryMiddleware):
    def pre_request(self, proxy, raw_args: Tuple, raw_kwargs: Dict):
        """
        In pre_request, args and kwargs are in the raw form before being
        processed by registry.on_request.
        """
        SqlalchemyDao.connect()
        SqlalchemyDao.begin()

        return (raw_args, raw_kwargs)

    def post_request(self, proxy, args: Tuple, kwargs: Dict, result):
        """
        In post_request, args and kwargs are in the form output by
        registry.on_request.
        """
        # TODO: pass in exc to post_request if there
        #   was an exception and rollback
        try:
            SqlalchemyDao.commit()
        except:
            SqlalchemyDao.rollback()
        finally:
            SqlalchemyDao.close()
