from typing import Dict, Tuple, Text, Type

from celery import Celery

from appyratus.utils.string_utils import StringUtils

from ravel.util import get_class_name, is_resource, is_batch
from ravel.app.base import Application, Action
from ravel.api import Api

DEFAULT_BROKER = 'amqp://'


class CeleryTask(Action):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._celery_task = None  # <- set in CeleryTaskManager.on_bootstrap

    @property
    def celery_task(self):
        return self._celery_task

    def s(self, *args, **kwargs):
        return self._celery_task.s(*args, **kwargs)

    def register_with_celery(self):
        # we must define a new function as the task target because
        # celery expects a __name__ attribute, which an Action object
        # itself does not have.
        func = lambda *args, **kwargs: self(*args, **kwargs)
        func.__name__ = f'{self.name}_task_function'

        task = self.app.celery.task(**self.decorator.kwargs)
        self._celery_task = task(func)

    def delay(self, *args, **kwargs):
        args, kwargs = self._prepare_celery_task_arguments(args, kwargs)
        return self._celery_task.delay(*args, **kwargs)

    def _prepare_celery_task_arguments(self, args, kwargs):
        args = args or tuple()
        kwargs = kwargs or {}

        def process_value(obj):
            if is_resource(obj):
                if obj._id is not None:
                    return obj._id
                else:
                    return obj.internal.state
            elif is_batch(obj):
                raise NotImplemented('TODO: implement support for batch objects')
            else:
                return obj

        if args and isinstance(args[0], Api):
            # ignore the "self" argument of the Api instance
            # whose method is registered as this action's callable.
            args = args[1:]

        new_args = [process_value(x) for x in args]
        new_kwargs = {k: process_value(v) for k, v in kwargs.items()}
        return (new_args, new_kwargs)
