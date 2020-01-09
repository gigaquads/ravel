from typing import List, Dict, Text, Type, Set, Tuple

from appyratus.env import Environment
from pybiz.app.middleware import Middleware, MiddlewareError
from pybiz.util.misc_functions import get_class_name
from pybiz.util.loggers import console

from .dao import SqlalchemyDao


class SqlalchemyMiddleware(Middleware):
    """
    Manages a Sqlalchemy database transaction that encompasses the execution of
    an Endpoint.
    """
    def __init__(self, dao_class_name: Text = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.env = Environment()
        self.dao_class_name = dao_class_name or self.env.get(
            'PYBIZ_SQLALCHEMY_DAO_CLASS', 'SqlalchemyDao'
        )

    def on_bootstrap(self):
        self.SqlalchemyDao = self.app.dal.get(self.dao_class_name)
        if self.SqlalchemyDao is None:
            raise MiddlewareError(self, 'SqlalchemyDao class not found')

    def pre_request(
        self,
        endpoint: 'Endpoint',
        raw_args: Tuple,
        raw_kwargs: Dict
    ):
        """
        Get a connection from Sqlalchemy's connection pool and begin a
        transaction.
        """
        self.SqlalchemyDao.connect()
        self.SqlalchemyDao.begin()

    def post_request(
        self,
        endpoint: 'Endpoint',
        raw_args: Tuple,
        raw_kwargs: Dict,
        processed_args: Tuple,
        processed_kwargs: Dict,
        result,
        exc: Exception = None,
    ):
        """
        Commit or rollback the tranaction.
        """
        # TODO: pass in exc to post_request if there
        #   was an exception and rollback
        try:
            if exc is not None:
                raise exc
            console.debug(
                f'{get_class_name(self)} trying to commit transaction'
            )
            self.SqlalchemyDao.commit()
        except:
            console.error(
                f'{get_class_name(self)} rolling back transaction'
            )
            self.SqlalchemyDao.rollback()
        finally:
            self.SqlalchemyDao.close()
