from typing import Dict, Tuple, Text, Type, Callable
from celery.signals import worker_process_init

import kombu.serialization

from mock import MagicMock
from celery import Celery

from appyratus.utils.string_utils import StringUtils
from appyratus.memoize import memoized_property

from ravel.util import get_class_name, is_resource, is_batch
from ravel.util.scanner import Scanner
from ravel.util.loggers import console
from ravel.manifest import Manifest
from ravel.app.base import Application, Action

from .celery_task import CeleryTask

DEFAULT_BROKER = 'amqp://'


class CeleryService(Application):

    def __init__(self, uses_client=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = CeleryClient(self) if uses_client else None
        self._celery = None

    @property
    def action_type(self) -> Type['CeleryTask']:
        return CeleryTask

    @property
    def celery_config(self) -> Dict:
        return self.local.manifest.data.get('celery', {})

    @property
    def client(self) -> 'CeleryClient':
        if self._client is None:
            self._client = CeleryClient(self)
        return self._client

    @property
    def celery(self) -> Celery:
        return self._celery

    def on_bootstrap(self, *args, **kwargs):
        self.initialize_celery()

    def on_start(self) -> Celery:
        return self.celery

    def initialize_celery(self):
        self._init_celery_app()
        self._init_celery_json_serializer()
        for action in self.actions.values():
            action.register_with_celery()

    def _init_celery_app(self):
        name = self.local.manifest.package
        broker = self.celery_config.setdefault('broker', DEFAULT_BROKER)
        backend = self.celery_config.get('result_backend')
        self._celery = Celery(name, broker=broker, backend=backend)
        self._celery.conf.update(self.celery_config)

        @worker_process_init.connect
        def bootstrap_celery_worker_process(*args, **kwargs):
            console.info('bootstrapping celery worker process')
            self.bootstrap(self.local.manifest)

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
                message=f'celery client does not recognize task name',
                data={'task': method}
            )
            raise ValueError(method)
        return method

    def bootstrap(self, manifest: 'Manifest', mocked=False) -> 'CeleryClient':
        # discover actions registered with app via decorator
        manifest = Manifest(manifest)
        self.scanner.scan(manifest.package)

        if not mocked:
            # trigger creation of the native Celery task objects
            self.app.manifest = manifest
            self.app.initialize_celery()
            console.info(
                message=f'initializing Celery client',
                data={
                    'broker': self.app.celery.conf.broker_url,
                    'tasks': sorted(self.app.actions.keys()),
                }
            )
            for action in self.app.actions.values():
                action.bootstrap()
                self.methods[action.name] = action
        else:
            # if "mocked", do not bother doing anything but
            # creating mock client methods
            for action in self.app.actions.values():
                self.methods[action.name] = MagicMock()

        return self
