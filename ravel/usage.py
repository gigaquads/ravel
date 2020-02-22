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
        data: Dict = None,
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
        if not isinstance(self._endpoint, str):
            pass
            #self._app = self._endpoint.app
        base_context = {
            'entrypoint': 'mrs-doc',    #self._entrypoint,
            'app': 'cli',    #self._app,
            'endpoint': self._endpoint.__name__,
            'fields': self._fields,
            'data': self._data,
        }
        context = self._merge_context(base_context, context)
        return super().render(context=context, **kwargs)


class CliApplicationUsageRenderer(ApplicationUsageRenderer):

    SPLIT = " \\\n"

    @classmethod
    def get_template(cls):
        #return "{entrypoint} {app} {endpoint} {args} {kwargs}"
        return "{cli_display}"

    def perform(self, context: Dict = None, **kwargs):
        if context is None:
            context = {}
        ctx = self.get_context()
        ctx.update(context)

        # build a shell command
        # the first item is the script
        # the remaining items are arguments and kwarguments
        command = [ctx['entrypoint'], ctx['app'], ctx['endpoint']]
        args = self.build_args(ctx['args'])
        kwargs = self.build_kwargs(ctx['kwargs'])
        command.extend(kwargs)
        # optionally indent every line but the first
        indent_args = True
        indent = '  '
        if indent_args:
            command[1:] = [f'{indent}{k}' for k in command[1:]]
        ctx['cli_display'] = self.SPLIT.join(command)
        return super().perform(context=ctx)

        # format shell script

    @classmethod
    def get_context(cls):
        return {
            'app': None,
            'entrypoint': None,
            'endpoint': None,
            'args': [],
            'kwargs': {},
        }

    def build_args(self, args=None):
        if not args:
            return []
        return args

    def build_kwargs(self, kwargs=None, split_newline=True):
        if not kwargs:
            return []
        flat_kwargs = DictUtils.flatten_keys(kwargs)
        kwargz = ['--{} {}'.format(k, v) for k, v in flat_kwargs.items() if v is not None]
        return kwargz


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
