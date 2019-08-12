import inspect

from pprint import pprint

from IPython.terminal.embed import InteractiveShellEmbed

from appyratus.cli import CliProgram, OptionalArg, PositionalArg, Subparser
from appyratus.files import Yaml
from appyratus.utils import StringUtils, SysUtils

from pybiz.util.misc_functions import is_bizlist, is_bizobj

from .base import Api, ApiDecorator, ApiProxy


class Cli(Api):
    """
    This Api subclass will create a CliProgram (command-line
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
        return obj.dump()
    elif isinstance(obj, (list, set, tuple)):
        return [_dump_result_obj(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: _dump_result_obj(v) for k, v in obj.items()}
    else:
        return obj


class CliCommand(ApiProxy):
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
        decorator_args = decorator.kwargs.get('args')
        cli_args = self._build_cli_args(func, decorator_args)
        name = StringUtils.dash(parser_kwargs.get('name') or self.program_name)
        return dict(parser_kwargs, **dict(
            name=name,
            args=cli_args,
            perform=self,
        ))

    def _build_cli_args(self, func, custom_args: list = None):
        required_args = []
        optional_args = []
        args = []
        # custom arguments like ones provided from a decorator should take
        # precedent over signature-generated arguments.  we will
        if not custom_args:
            custom_args = []
        custom_args_by_name = {a.name: a for a in custom_args}
        params = self.signature.parameters
        # collect params by first character, in order to identify collisions
        params_by_char = {}
        for k, param in params.items():
            kchar = k[0]
            if kchar not in params_by_char:
                params_by_char[kchar] = []
            params_by_char[kchar].append(k)
        # now process the signature params
        for k, param in params.items():
            # determine dtype
            if param.annotation is inspect._empty:
                dtype = None
            else:
                dtype = param.annotation
            # conditionally set the dtype too if it was not provided in the
            # decorated arg, and that it exists throug the param annotation
            arg = custom_args_by_name.get(k)
            if arg:
                if not arg.dtype and dtype:
                    arg.dtype = dtype
                args.append(arg)
                continue
            # optional short flag can cause collisions with params beginning
            # with the same character, so only use the short flag
            relative_params = params_by_char.get(k[0])
            use_optional_short_flag = len(relative_params) == 1
            # normal signature argument processing
            arg = None
            if param.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD:
                if param.default is inspect._empty:
                    arg = PositionalArg(name=k, dtype=dtype)
                else:
                    arg = OptionalArg(name=k, dtype=dtype, short_flag=use_optional_short_flag)
            elif param.kind == inspect.Parameter.POSITIONAL_ONLY:
                arg = PositionalArg(name=k, dtype=dtype)
            elif param.kind == inspect.Parameter.KEYWORD_ONLY:
                arg = OptionalArg(name=k, dtype=dtype, short_flag=use_optional_short_flag)
            if arg is not None:
                args.append(arg)

        return args
