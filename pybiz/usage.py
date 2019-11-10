from typing import (
    Dict,
    Text,
)

from appyratus.utils import DictUtils
from mrs_doc.usage import (
    BaseUsage,
    BaseUsageRenderer,
)


class ApplicationUsageRenderer(BaseUsageRenderer):
    pass


class ApplicationUsage(BaseUsage):

    def __init__(
        self,
        app: Text = None,
        entrypoint: Text = None,
        endpoint=None,
        fields=None,
        data=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self._app = app
        self._entrypoint = entrypoint
        self._endpoint = endpoint
        self._data = data
        self._fields = fields
        self._renderer = None

    def render(self, context: Dict = None, **kwargs):
        base_context = {
            'app': self._app,
            'fields': self._fields,
            'data': self._data,
            'entrypoint': self._entrypoint,
            'endpoint': self._endpoint,
        }
        context = self._merge_context(base_context, context)
        return super().render(context=context, **kwargs)


class CliApplicationUsageRenderer(ApplicationUsageRenderer):

    @classmethod
    def get_template(cls):
        return "{entrypoint} {app} {endpoint} {args} {kwargs}"

    def perform(self, context: Dict = None, **kwargs):
        if context is None:
            context = {}
        ctx = self.get_context()
        ctx.update(context)
        ctx['args'] = self.build_args(ctx['args'])
        ctx['kwargs'] = self.build_kwargs(ctx['data'])
        return super().perform(context=ctx, **kwargs)

    @classmethod
    def get_context(cls):
        return {
            'app': None,
            'entrypoint': None,
            'endpoint': None,
            'args': [],
            'kwargs': {},
        }

    def build_args(self, args):
        if not args:
            return ''
        return ' '.join(args)

    def build_kwargs(self, kwargs):
        if not kwargs:
            return ''
        flat_kwargs = DictUtils.flatten_keys(kwargs)
        kwargs = ['--{} {}'.format(k, v) for k, v in flat_kwargs.items() if v is not None]
        return ' '.join(kwargs)

"""

ApplicationUsage(
    name='Scan your local subnet (255 hosts) for open connections on port 80',
    description='',
    endpoint='port-scanner',
    fields={
        'subnet': str,
        'port': str,
    },
    data={
        'subnet': '192.168.1.0/24',
        'port': '80',
    }
)

ApplicationUsage(
    name='Scan a single host on your network for any open ports (1-65535)',
    description='',
    endpoint='port-scanner',
    data={
        'subnet': '192.168.1.1',
        'port': '*',
    }
)
"""

APPLICATION_RENDERERS = {}
