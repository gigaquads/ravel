import inspect

from IPython.terminal.embed import InteractiveShellEmbed
from appyratus.cli import CliProgram, Subparser as Parser, Arg

from .base import FunctionRegistry, FunctionDecorator, FunctionProxy


class Repl(FunctionRegistry):
    """
    Repl is a FunctionRegistry that collects all registered functions and
    injects them into an interactive Python shell, or REPL. This is useful for
    experimenting with an API from a command-line interface.
    """

    def __init__(
        self,
        name=None,
        version=None,
        tagline=None,
        defaults=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._proxies = []  # FunctionProxy objects wrap decorated functions
        self._ipython = None
        self._cli_program_kwargs = {
            'name': name,
            'version': version,
            'tagline': tagline,
            'defaults': defaults,
        }

    @property
    def function_proxy_type(self):
        return ReplFunctionProxy

    def on_decorate(self, proxy):
        """
        Collect each FunctionProxy, which contains a reference to a function
        we're going to inject into the namespace of the REPL.
        """
        self._proxies.append(proxy)

    def on_request(self, signature, *args, **kwargs):
        if args and isinstance(args[0], CliProgram):
            prog = args[0]
            arguments = prog.args
            args, kwargs = [], {}

            for k, param in signature.parameters.items():
                value = getattr(arguments, k, None)
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

    def start(self, interactive=True, *args, **kwargs):
        """
        Start a new REPL with all registered functions available in the REPL
        namespace.
        """
        if interactive:
            self._ipython = InteractiveShellEmbed()
            self._ipython.mainloop(local_ns=dict(
                {p.func_name: p.func for p in self._proxies},
                repl=self
            ))
        else:
            class DynamicCliProgram(CliProgram):
                @staticmethod
                def subparsers():
                    return [p.parser for p in self._proxies if p.parser]

            prog = DynamicCliProgram(**self._cli_program_kwargs)
            prog.run()

    @property
    def function_names(self):
        """
        Get list of names of all registered functions in the REPL.
        """
        return sorted(p.func_name for p in self._proxies)


class ReplFunctionProxy(FunctionProxy):
    def __init__(self, func, decorator):
        super().__init__(func, decorator)
        self.parser = decorator.params.get('parser')
        if self.parser is not None:
            self.parser.perform = self
