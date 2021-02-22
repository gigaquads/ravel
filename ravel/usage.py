from typing import (
    Dict,
    Text,
)

from appyratus.usage import (
    BaseUsage,
    BaseUsageRenderer,
)
from appyratus.utils.dict_utils import DictUtils
from appyratus.utils.string_utils import StringUtils

from ravel.apps.cli import CliApplication


class ApplicationUsage(BaseUsage):

    def __init__(
        self,
        package: Text = None,
        apps: Text = None,
        entrypoint: Text = None,
        action=None,
        fields=None,
        data: Dict = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self._package = package
        self._apps = apps
        self._entrypoint = entrypoint
        self._action = action
        self._data = data
        self._fields = fields

    def render(self, context: Dict = None, **kwargs):
        base_context = {
            'fields': self._fields,
            'data': self._data,
        }
        context = self._merge_context(base_context, context)
        res = super().render(context=context, **kwargs)
        return res


class ApplicationUsageRenderer(BaseUsageRenderer):
    pass


class CliApplicationUsageRenderer(ApplicationUsageRenderer):
    """
    # Cli Application Usage Renderer
    Renders command line code for shell interpreters
    """

    SPLIT = " \\\n"
    INDENT = "  "

    def __init__(
        self, split_args=False, split_kwargs=True, indent_after_command=True, **kwargs
    ):
        """
        # Init
        # Args 
        - `split_args`, split up each argument into a new line
        - `split_kwargs`, split up each kwarg into a new line
        - `indent_after_command`, Following the command on the first line,
          indent subsequent elements into a new line with proper escape
        """
        self._split_args = split_args
        self._split_kwargs = split_kwargs
        self._indent_after_command = indent_after_command
        super().__init__(**kwargs)

    @classmethod
    def get_template(cls):
        return "{cli_display}"

    def perform(self, context: Dict = None, **kwargs):
        if context is None:
            context = {}
        ctx = self.get_context()
        ctx.update(
            {
                'app': 'cli',
                'entrypoint': StringUtils.dash(self.usage._package.__name__),
                'action': StringUtils.dash(self.usage._action.__name__),
            }
        )
        ctx.update(context)

        # build a shell command
        # the first item is the command
        # the remaining items are arguments and kwarguments
        command = ctx['entrypoint']
        args = self.build_args([ctx['app'], ctx['action']] + ctx['args'])
        kwargs = self.build_kwargs(ctx['kwargs'])
        command_line = [command]
        command_line.extend(args)
        command_line.extend(kwargs)

        # optionally indent every line but the first
        if self._indent_after_command:
            command_line[1:] = [f'{self.INDENT}{k}' for k in command_line[1:]]
        # set the value of cli display for when rendering the template
        ctx['cli_display'] = self.SPLIT.join(command_line)
        # finally render the template
        return super().perform(context=ctx)

    @classmethod
    def get_context(cls):
        return {
            'app': None,
            'entrypoint': None,
            'action': None,
            'args': [],
            'kwargs': {},
        }

    def build_args(self, args=None):
        """
        # Build Args
        """
        if not args:
            return []
        if not self._split_args:
            return [' '.join(args)]
        return args

    def build_kwargs(self, kwargs=None, split_newline=True):
        """
        # Build Kwargs
        """
        if not kwargs:
            return []
        flat_kwargs = DictUtils.flatten_keys(kwargs)
        kwargz = ['--{} {}'.format(k, v) for k, v in flat_kwargs.items() if v is not None]
        if not self._split_kwargs:
            return [' '.join(kwargz)]
        return kwargz
