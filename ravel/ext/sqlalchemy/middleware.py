from typing import List, Dict, Text, Type, Set, Tuple

from appyratus.env import Environment
from ravel.app.middleware import Middleware, MiddlewareError
from ravel.util.misc_functions import get_class_name
from ravel.util.loggers import console

from .store import SqlalchemyStore

ADD_POST_COMMIT_HOOK_METHOD = 'add_post_commit_hook'
POST_COMMIT_HOOKS = 'ManageSqlalchemyTransaction_post_commit_hooks'

class ManageSqlalchemyTransaction(Middleware):
    """
    Manages a Sqlalchemy database transaction that encompasses the execution of
    an Action.
    """
    def __init__(self, store_class_name: Text = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.env = Environment()
        self.store_class_name = store_class_name or 'SqlalchemyStore'

    def on_bootstrap(self):
        store_types = self.app.storage.store_types
        self.store_type = store_types.get(self.store_class_name)
        if self.store_type is None:
            raise MiddlewareError(
                self, f'{self.store_class_name} class not found'
            )

    def pre_request(
        self,
        action: 'Action',
        request: 'Request',
        raw_args: Tuple,
        raw_kwargs: Dict
    ):
        """
        Get a connection from Sqlalchemy's connection pool and begin a
        transaction.
        """
        # re-init post-commit hooks list
        hooks = []
        request.context[POST_COMMIT_HOOKS] = hooks
        request.context[ADD_POST_COMMIT_HOOK_METHOD] = func = (
            lambda hook, args=(), kwargs={}: hooks.append(
                [hook, args, kwargs]
            )
        )

        setattr(self.app.local, POST_COMMIT_HOOKS, hooks)
        setattr(self.app.local, ADD_POST_COMMIT_HOOK_METHOD, func)

        self.store_type.connect()
        self.store_type.begin()

    def post_request(
        self,
        action: 'Action',
        request: 'Request',
        result,
    ):
        """
        Commit or rollback the tranaction.
        """
        console.debug(f'committing sqlalchemy transaction')
        try:
            self.store_type.commit()

            # execute post-commit hooks
            for hook, args, kwargs in request.context[POST_COMMIT_HOOKS]:
                hook(*args, **kwargs)
        except Exception:
            console.exception(f'rolling back sqlalchemy transaction')
            self.store_type.rollback()
        finally:
            self.store_type.close()

    def post_bad_request(
        self,
        action: 'Action',
        request: 'Request',
        exc: Exception,
    ):
        console.info(f'rolling back sqlalchemy transaction')
        try:
            self.store_type.rollback()
        finally:
            self.store_type.close()
