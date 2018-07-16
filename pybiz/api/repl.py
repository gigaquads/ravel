from IPython.terminal.embed import InteractiveShellEmbed

from .base import FunctionRegistry


class Repl(FunctionRegistry):
    """
    Repl is a FunctionRegistry that collects all registered functions and
    injects them into an interactive Python shell, or REPL. This is useful for
    experimenting with an API from a command-line interface.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ipython = InteractiveShellEmbed()
        self._proxies = []  # FunctionProxy objects wrap decorated functions

    def hook(self, proxy):
        """
        Collect each FunctionProxy, which contains a reference to a function
        we're going to inject into the namespace of the REPL.
        """
        self._proxies.append(proxy)

    def start(self, *args, **kwargs):
        """
        Start a new REPL with all registered functions available in the REPL
        namespace.
        """
        self._ipython.mainloop(local_ns=dict(
            {p.func_name: p.func for p in self._proxies},
            repl=self
        ))

    @property
    def function_names(self):
        """
        Get list of names of all registered functions in the REPL.
        """
        return sorted(p.func_name for p in self._proxies)

