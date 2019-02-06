from typing import List, Dict, Text, Type, Set, Tuple

from pybiz.api.middleware import RegistryMiddleware


class SqlalchemyMiddleware(RegistryMiddleware):

    def pre_request(self, proxy, args: Tuple, kwargs: Dict):
        """
        In pre_request, args and kwargs are in the raw form before being
        processed by registry.on_request.
        """
        from pybiz.dao.sqlalchemy_dao import SqlalchemyDao

        SqlalchemyDao.connect()
        SqlalchemyDao.begin()

    def post_request(self, proxy, args: Tuple, kwargs: Dict, result):
        """
        In post_request, args and kwargs are in the form output by
        registry.on_request.
        """
        from pybiz.dao.sqlalchemy_dao import SqlalchemyDao

        # TODO: pass in exc to post_request if there
        #   was an exception and rollback
        try:
            SqlalchemyDao.commit()
        except:
            SqlalchemyDao.rollback()
        finally:
            SqlalchemyDao.close()
