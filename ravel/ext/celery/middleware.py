from ravel.app.middleware import Middleware


class ManageCeleryClient(Middleware):
    def __init__(self, service: 'CeleryService', mocked=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._celery_service = service
        self._mocked = mocked

    def on_bootstrap(self):
        """
        Initialize a client for the given CeleryService
        """
        self.app.local.celery = self._celery_service.client.bootstrap(
            manifest=self.app.manifest,
            mocked=self._mocked,
        )
