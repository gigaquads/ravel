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
        """
        Aqcuire the Store subclass object from self.store_class_name.
        """
        store_types = self.app.manifest.store_classes
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
        hooks = []

        # add a dynamic add_post_commit_hook method to request.context
        # for use in Actions that register post-commit hooks.
        request.context[POST_COMMIT_HOOKS] = hooks
        request.context[ADD_POST_COMMIT_HOOK_METHOD] = func = (
            lambda hook, args=(), kwargs={}: hooks.append(
                [hook, args, kwargs]
            )
        )
        # add post-commit hook storage containers to
        # application thread-local state
        setattr(self.app.local, POST_COMMIT_HOOKS, hooks)
        setattr(self.app.local, ADD_POST_COMMIT_HOOK_METHOD, func)

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
        self.store_type.commit(rollback=True)

        # execute post-commit hooks in background processes
        for hook, args, kwargs in request.context[POST_COMMIT_HOOKS]:
            self.app.spawn(
                hook, args=args, kwargs=kwargs, multiprocessing=True
            )

    def post_bad_request(
        self,
        action: 'Action',
        request: 'Request',
        exc: Exception,
    ):
        """
        Rollback a failed transaction.
        """
        console.info(f'rolling back sqlalchemy transaction')
        try:
            self.store_type.rollback()
        finally:
            self.store_type.close()
