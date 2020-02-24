import inspect

from pprint import pprint
from typing import List
from IPython.terminal.embed import InteractiveShellEmbed
from appyratus.cli import (
    CliProgram,
    OptionalArg,
    PositionalArg,
    ListArg,
    FlagArg,
    Subparser,
)
from appyratus.files import Yaml
from appyratus.utils import StringUtils, SysUtils

from ravel.util import is_batch, is_resource
from ravel.app.base import Application, EndpointDecorator, Endpoint


class CliApplication(Application):
    """
    This Application subclass will create a CliProgram (command-line
    interace) out of the registered functions.
    """

    def __init__(
        self,
        name=None,
        version=None,
        tagline=None,
        defaults=None,
        echo=False,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._commands = []
        self._echo = echo
        self._cli_program = None
        self._cli_args = None
        self._cli_program_kwargs = {
            'name': name,
            'version': version,
            'tagline': tagline,
            'defaults': defaults,
        }

    @property
    def endpoint_type(self):
        return CliCommand

    def on_bootstrap(self, cli_args=None):
        """
        Collect subparsers and build cli program
        """
        self._cli_args = cli_args
        subparsers = [c.subparser for c in self.endpoints.values() if c.subparser]
        self._cli_program = CliProgram(
            subparsers=subparsers, cli_args=self._cli_args, **self._cli_program_kwargs
        )

    def on_start(self):
        """
        Run the CliProgram.
        """
        SysUtils.safe_main(self._cli_program.run, debug_level=2)

    def on_request(self, endpoint, *args, **kwargs):
        """
        Extract command line arguments and bind them to the arguments expected
        by the registered function's signature.
        """
        args, kwargs = [], {}
        for k, param in endpoint.signature.parameters.items():
            value = getattr(self._cli_program.cli_args, k, None)
            if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                if param.default is inspect._empty:
                    args.append(value)
                else:
                    kwargs[k] = value
            elif param.kind == inspect.Parameter.POSITIONAL_ONLY:
                args.append(value)
            elif param.kind == inspect.Parameter.KEYWORD_ONLY:
                kwargs[k] = value
        return (args, kwargs)

    def on_response(self, endpoint, result, *args, **kwargs):
        response = super().on_response(endpoint, result, *args, **kwargs)
        dumped_result = _dump_result_obj(response)
        if self._echo:
            output_format = getattr(self._cli_program.cli_args, 'format', None)
            formatted_result = _format_result_data(dumped_result, output_format)
            if isinstance(formatted_result, str):
                print(formatted_result)
            else:
                pprint(formatted_result)
        return dumped_result


def _format_result_data(data, output_format):
    if output_format == 'yaml':
        return Yaml.from_data(data)
    else:
        return data


def _dump_result_obj(obj):
    if is_resource(obj) or is_batch(obj):
        return obj.dump()
    elif isinstance(obj, (list, set, tuple)):
        return [_dump_result_obj(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: _dump_result_obj(v) for k, v in obj.items()}
    else:
        return obj


class CliCommand(Endpoint):
    """
    CliCommand represents a top-level CliProgram Subparser.
    """

    def __init__(self, func, decorator):
        super().__init__(func, decorator)
        self.program_name = None
        self.subparser_kwargs = None
        self.subparser_type = None
        self.subparser = None

    def on_bootstrap(self):
        self.program_name = self.decorator.kwargs.get('name', self.name)
        self.subparser_kwargs = self._build_subparser_kwargs(self.exc)
        self.subparser_type = self.decorator.kwargs.get('subparser', Subparser)
        self.subparser = self.subparser_type(**self.subparser_kwargs)

    def _build_subparser_kwargs(self, func):
        parser_kwargs = self.decorator.kwargs.get('parser') or {}
        custom_args = self.decorator.kwargs.get('args') or []
        cli_args = self._build_cli_args(func, custom_args)
        name = StringUtils.dash(parser_kwargs.get('name') or self.program_name)
        return dict(parser_kwargs, **dict(
            name=name,
            args=cli_args,
            perform=self,
        ))

    def _build_cli_args(self, func, custom_args: List = None):
        if custom_args is None:
            custom_args = []
        required_args = []
        optional_args = []
        args = []
        custom_args_by_name = {a.name: a for a in custom_args}
        for k, param in self.signature.parameters.items():
            if k in custom_args_by_name.keys():
                # if a custom argument was passed into the decorator, then this
                # should take precedent over the inferred arg created in this
                # iteration
                args.append(custom_args_by_name[k])
                continue

            arg = None
            arg_type = None
            if param.annotation is inspect._empty:
                dtype = None
            else:
                dtype = param.annotation
            arg_params = {
                'name': k,
                'dtype': dtype,
            }

            if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                if param.default is inspect._empty:
                    arg_type = PositionalArg
                else:
                    arg_type = OptionalArg
            elif param.kind == inspect.Parameter.POSITIONAL_ONLY:
                arg_type = PositionalArg
            elif param.kind == inspect.Parameter.KEYWORD_ONLY:
                arg_type = OptionalArg
            if 'List' in str(dtype):
                arg_type = ListArg
            elif 'bool' in str(dtype):
                arg_type = FlagArg
            if arg_type:
                arg = arg_type(**arg_params)
            if arg is not None:
                args.append(arg)

        return args
