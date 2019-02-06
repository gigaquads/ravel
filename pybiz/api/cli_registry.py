import inspect

from pprint import pprint

from IPython.terminal.embed import InteractiveShellEmbed
from appyratus.utils import SysUtils
from appyratus.cli import (
    CliProgram,
    Subparser,
    OptionalArg,
    PositionalArg,
)

from .registry import Registry, RegistryDecorator, RegistryProxy


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
        self._cli_program_kwargs = {
            'name': name,
            'version': version,
            'tagline': tagline,
            'defaults': defaults,
        }

    @property
    def proxy_type(self):
        return CliCommand

    def on_start(self):
        """
        Build and run the CliProgram.
        """
        subparsers = [
            c.subparser for c in self.proxies.values() if c.subparser
        ]
        self._cli_program = CliProgram(
            subparsers=subparsers, **self._cli_program_kwargs
        )
        SysUtils.safe_main(self._cli_program.run, debug_level=2)

    def on_request(self, command, prog, *args, **kwargs):
        """
        Extract command line arguments and bind them to the arguments expected
        by the registered function's signature.
        """
        args, kwargs = [], {}
        for k, param in command.signature.parameters.items():
            value = getattr(prog.cli_args, k, None)
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
        if self._echo:
            pprint(result)
        response = super().on_response(proxy, result, *args, **kwargs)
        return response


class CliCommand(RegistryProxy):
    """
    CliCommand represents a top-level CliProgram Subparser.
    """

    def __init__(self, func, decorator):
        super().__init__(func, decorator)
        self.subparser_kwargs = self._build_subparser_kwargs(func, decorator)
        self.subparser = Subparser(**self.subparser_kwargs)

    def _build_subparser_kwargs(self, func, decorator):
        parser_kwargs = decorator.kwargs.get('parser') or {}
        cli_args = self._build_cli_args(func)
        return dict(
            parser_kwargs,
            **dict(
                name=parser_kwargs.get(
                    'name', func.__name__.replace('_', '-')
                ),
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
