from typing import Dict, Tuple, Text, Type, Callable

import kombu.serialization

from mock import MagicMock
from celery import Celery

from appyratus.utils import StringUtils
from appyratus.memoize import memoized_property

from ravel.util import get_class_name, is_resource, is_batch
from ravel.util.scanner import Scanner
from ravel.util.loggers import console
from ravel.manifest import Manifest
from ravel.app.base import Application, Action

from .celery_task import CeleryTask

DEFAULT_BROKER = 'amqp://'


class CeleryService(Application):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._celery = None
        self._client = CeleryClient(self)

    @property
    def action_type(self) -> Type['CeleryTask']:
        return CeleryTask

    @property
    def options(self) -> Dict:
        return self.manifest.get('celery', {})

    @property
    def client(self) -> 'CeleryClient':
        return self._client

    @property
    def celery(self) -> Celery:
        return self._celery

    def on_bootstrap(self, *args, **kwargs):
        self._init_celery_app()
        self._init_celery_json_serializer()

    def on_start(self):
        return self.celery

    def _init_celery_app(self):
        broker = self.options.setdefault('broker', DEFAULT_BROKER)
        name = self.manifest.get('package')
        self._celery = Celery(name, broker=broker)
        self._celery.conf.update(self.options)

    def _init_celery_json_serializer(self):
        serializer_name = 'json'
        kombu.serialization.register(
            serializer_name,
            self.json.encode,
            self.json.decode,
            content_type='application/json',
            content_encoding='utf-8',
        )
        self._celery.task_serializer = serializer_name
        self._celery.conf.update(
            task_serializer=serializer_name,
            accept_content=[serializer_name],
            result_serializer=serializer_name,
        )



class CeleryClient(object):
    def __init__(self, app):
        self.app = app
        self.scanner = Scanner()
        self.methods = {}

    def __getattr__(self, method: Text) -> Callable:
        method = self.methods.get(method)
        if method is None:
            console.error(
                message=f'requested unrecognized Celery task',
                data={'task': method}
            )
            raise ValueError(method)
        return method

    def bootstrap(self, manifest: 'Manifest', mocked=False) -> 'CeleryClient':
        # discover actions registered with app via decorator
        self.scanner.scan(manifest.package)

        if not mocked:
            # trigger creation of the native Celery task objects
            self.app.manifest = manifest
            self.app.on_bootstrap()
            console.info(
                message=f'initializing Celery client',
                data={
                    'broker': self.app.celery.conf.broker_url,
                    'tasks': sorted(self.app.actions.keys()),
                }
            )
            for action in self.app.actions.values():
                action.bootstrap()
                self.methods[action.name] = action.delay
        else:
            # if "mocked", do not bother doing anything but
            # creating mock client methods
            for action in self.app.actions.values():
                self.methods[action.name] = MagicMock()

        return self
