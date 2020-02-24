from typing import Dict, Tuple, Text

from celery import Celery

from appyratus.utils import StringUtils

from ravel.util import get_class_name, is_resource, is_batch
from ravel.api import Api
from ravel.app.base import Application, Action


DEFAULT_BROKER = 'amqp://'


class CeleryApplication(Application):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.celery_app = None
        self.options = {}

    @property
    def action_type(self):
        return CeleryTask

    @property
    def celery_app_name(self):
        return (
            self.options.get('app_name')
            or self.manifest.get('package')
            or StringUtils.snake(get_class_name(self))
        )

    @property
    def celery_broker(self):
        return self.options.get('broker', DEFAULT_BROKER)

    def on_bootstrap(self, *args, **kwargs):
        # initialize the Celery application object
        self.celery_app = Celery(
            self.celery_app_name,
            broker=self.celery_broker
        )
        # register actions as Tasks with Celery
        for action in self.api.values():
            func = lambda *args, **kwargs: action(*args, **kwargs)
            func.__name__ = action.name

            task = self.celery_app.task(**action.decorator.kwargs)
            action.celery_task = task(func)

    def on_request(
        self,
        action: 'Action',
        *raw_args,
        **raw_kwargs
    ) -> Tuple[Tuple, Dict]:
        args, kwargs = super().on_request(action, *raw_args, **raw_kwargs)
        print(raw_args, raw_kwargs, args, kwargs)
        return (args, kwargs)

    def on_response(
        self,
        action: 'Action',
        raw_result: object,
        *raw_args,
        **raw_kwargs
    ):
        # since we're calling celery async inside, we don't
        # return any result synchronously.
        return None

    def on_start(self):
        return self.celery_app


class CeleryTask(Action):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.celery_task = None

    def apply_async(self, args=None, kwargs=None, **options):
        args, kwargs = self._prepare_celery_task_arguments(args, kwargs)
        return self.celery_task.apply_async(args=args, kwargs=kwargs, **options)

    def delay(self, *args, **kwargs):
        args, kwargs = self._prepare_celery_task_arguments(args, kwargs)
        return self.celery_task.delay(*args, **kwargs)

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
            args = args[1:]

        new_args = [process_value(x) for x in args]
        new_kwargs = {k: process_value(v) for k, v in kwargs.items()}
        return (new_args, new_kwargs)
