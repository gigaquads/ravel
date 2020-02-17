from appyratus.utils import StringUtils

from ravel.util import get_class_name
from ravel.app.base import Application, Endpoint


DEFAULT_BROKER = 'amqp://guest@localhost//'


class CeleryApplication(Application):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.celery_app = None
        self.options = {}

    @property
    def endpoint_type(self):
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

    def on_bootstsrap(self, *args, **kwargs):
        # initialize the Celery application object
        self.celery_app = Celery(
            self.celery_app_name,
            broker=self.celery_broker
        )
        # register endpoints as Tasks with Celery
        for endpoint in self.api.values():
            self.celery_app.task(self)

    def on_start(self):
        return self.celery_app


class CeleryTask(Endpoint):
    pass
