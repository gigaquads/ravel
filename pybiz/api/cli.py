import inspect

from IPython.terminal.embed import InteractiveShellEmbed
from appyratus.cli import (
    CliProgram,
    Subparser,
    OptionalArg,
    PositionalArg,
    safe_main,
)

from .base import FunctionRegistry, FunctionDecorator, FunctionProxy


class CliApplication(FunctionRegistry):
    """
    This FunctionRegistry subclass will create a CliProgram (command-line
    interace) out of the registered functions.
    """

    def __init__(
        self,
        name=None,
        version=None,
        tagline=None,
        defaults=None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._commands = []
        self._cli_program = None
        self._cli_program_kwargs = {
            'name': name,
            'version': kwargs.get('version'),
            'tagline': kwargs.get('tagline'),
            'defaults': kwargs.get('defaults'),
        }

    @property
    def function_proxy_type(self):
        return Command

    def on_decorate(self, command):
        """
        Collect each FunctionProxy, which contains a reference to a function
        we're going to inject into the namespace of the REPL.
        """
        self._commands.append(command)

    def on_request(self, command, signature, prog, *args, **kwargs):
        """
        Extract command line arguments and bind them to the arguments expected
        by the registered function's signature.
        """
        args, kwargs = [], {}
        for k, param in signature.parameters.items():
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

    def start(self, *args, **kwargs):
        """
        Build and run the CliProgram.
        """
        self._cli_program = CliProgram(
            subparsers=[c.subparser for c in self._commands if c.subparser],
            **self._cli_program_kwargs
        )
        safe_main(self._cli_program.run, debug_level=2)


class Command(FunctionProxy):
    """
    Command represents a top-level CliProgram Subparser.
    """

    def __init__(self, func, decorator):
        super().__init__(func, decorator)
        self.subparser_kwargs = self._build_subparser_kwargs(func, decorator)
        self.subparser = Subparser(**self.subparser_kwargs)

    def _build_subparser_kwargs(self, func, decorator):
        parser_kwargs = decorator.params.get('parser') or {}
        args = self._build_cli_args(func)
        return dict(
            parser_kwargs,
            **dict(
                name=parser_kwargs.get(
                    'name', func.__name__.replace('_', '-')
                ),
                args=args,
                perform=self,
            )
        )

    def _build_cli_args(self, func):
        required_args = []
        optional_args = []
        signature = inspect.signature(func)
        args = []
        for k, param in signature.parameters.items():
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
