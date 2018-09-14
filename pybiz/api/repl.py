import inspect

from IPython.terminal.embed import InteractiveShellEmbed

from .base import FunctionRegistry, FunctionDecorator, FunctionProxy


class Repl(FunctionRegistry):
    """
    Repl is a FunctionRegistry that collects all registered functions and
    injects them into an interactive Python shell, or REPL. This is useful for
    experimenting with an API from a command-line interface.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shell = InteractiveShellEmbed()

    @property
    def function_proxy_type(self):
        return ReplFunctionProxy

    def on_decorate(self, proxy):
        pass

    def on_request(self, proxy, signature, *args, **kwargs):
        return (args, kwargs)

    def start(self, *args, **kwargs):
        """
        Start a new REPL with all registered functions available in the REPL
        namespace.
        """
        self.functions
        self.shell.mainloop(local_ns=self._build_shell_namespace())

    def _build_shell_namespace(self):
        ns = {'repl': self}
        ns.update({p.name: p for p in self.proxies})
        return ns

    @property
    def functions(self):
        """
        Get list of names of all registered functions in the REPL.
        """
        func_names = sorted(p.target.__name__ for p in self.proxies)
        for func_name in func_names:
            print('- {}'.format(func_name))


class ReplFunctionProxy(FunctionProxy):
    def __init__(self, func, decorator):
        super().__init__(func, decorator)

    def debug(self, *args, **kwargs):
        return self.call_target(args, kwargs, pybiz_debug=True)

    @property
    def source(self):
        print(inspect.getsource(self.target))
