import traceback

from pybiz.dao.sqlalchemy_dao import SqlalchemyDao

from .base import Middleware


class SqlalchemyDaoMiddleware(Middleware):
    def process_request(self, request, response):
        SqlalchemyDao.connect()
        SqlalchemyDao.begin()

    def process_resource(self, request, response, resource, params):
        pass

    def process_response(self, request, response, resource):
        try:
            if response.ok:
                SqlalchemyDao.commit()
            else:
                raise Exception()
        except Exception:
            SqlalchemyDao.rollback()
            traceback.print_exc()
        finally:
            SqlalchemyDao.close()
