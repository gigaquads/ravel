import inspect
from pprint import pprint

from IPython.terminal.embed import InteractiveShellEmbed
from appyratus.cli import (
    CliProgram,
    OptionalArg,
    PositionalArg,
    Subparser,
)
from appyratus.files import Yaml
from appyratus.utils import (
    StringUtils,
    SysUtils,
)
from pybiz.util import (
    is_bizlist,
    is_bizobj,
)

from .registry import (
    Registry,
    RegistryDecorator,
    RegistryProxy,
)


class CliRegistry(Registry):
    """
    This Registry subclass will create a CliProgram (command-line
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
    def proxy_type(self):
        return CliCommand

    def on_bootstrap(self, cli_args=None):
        """
        Collect subparsers and build cli program
        """
        self._cli_args = cli_args
        subparsers = [
            c.subparser for c in self.proxies.values() if c.subparser
        ]
        self._cli_program = CliProgram(
            subparsers=subparsers,
            cli_args=self._cli_args,
            **self._cli_program_kwargs
        )

    def on_start(self):
        """
        Run the CliProgram.
        """
        SysUtils.safe_main(self._cli_program.run, debug_level=2)

    def on_request(self, proxy, *args, **kwargs):
        """
        Extract command line arguments and bind them to the arguments expected
        by the registered function's signature.
        """
        args, kwargs = [], {}
        for k, param in proxy.signature.parameters.items():
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

    def on_response(self, proxy, result, *args, **kwargs):
        response = super().on_response(proxy, result, *args, **kwargs)
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
    if is_bizobj(obj) or is_bizlist(obj):
        return obj.dump(raw=True)
    elif isinstance(obj, (list, set, tuple)):
        return [_dump_result_obj(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: _dump_result_obj(v) for k, v in obj.items()}
    else:
        return obj


class CliCommand(RegistryProxy):
    """
    CliCommand represents a top-level CliProgram Subparser.
    """

    def __init__(self, func, decorator):
        super().__init__(func, decorator)
        self.program_name = decorator.kwargs.get('name', func.__name__)
        self.subparser_kwargs = self._build_subparser_kwargs(func, decorator)
        self.subparser = Subparser(**self.subparser_kwargs)

    def _build_subparser_kwargs(self, func, decorator):
        parser_kwargs = decorator.kwargs.get('parser') or {}
        cli_args = self._build_cli_args(func)
        name = StringUtils.dash(parser_kwargs.get('name') or self.program_name)
        return dict(
            parser_kwargs, **dict(
                name=name,
                args=cli_args,
                perform=self,
            )
        )

    def _build_cli_args(self, func):
        required_args = []
        optional_args = []
        args = []
        for k, param in self.signature.parameters.items():
            arg = None
            if param.annotation is inspect._empty:
                dtype = None
            else:
                dtype = param.annotation
            if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                if param.default is inspect._empty:
                    arg = PositionalArg(name=k, dtype=dtype)
                else:
                    arg = OptionalArg(name=k, dtype=dtype)
            elif param.kind == inspect.Parameter.POSITIONAL_ONLY:
                arg = PositionalArg(name=k, dtype=dtype)
            elif param.kind == inspect.Parameter.KEYWORD_ONLY:
                arg = OptionalArg(name=k, dtype=dtype)
            if arg is not None:
                args.append(arg)
        return args
