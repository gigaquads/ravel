import inspect

from typing import Dict, List, Text

from IPython.terminal.embed import InteractiveShellEmbed

from .registry import Registry, RegistryDecorator, RegistryProxy


class ReplRegistry(Registry):
    """
    Repl is a Registry that collects all registered functions and
    injects them into an interactive Python shell, or REPL. This is useful for
    experimenting with an API from a command-line interface.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shell = InteractiveShellEmbed()

    @property
    def proxy_type(self):
        return Function

    def on_decorate(self, proxy: 'Function'):
        pass

    def on_request(self, proxy: 'Function', signature, *args, **kwargs):
        return (args, kwargs)

    def start(self, namespace: Dict = None, *args, **kwargs):
        """
        Start a new REPL with all registered functions available in the REPL
        namespace.
        """
        # build the shell namespace
        local_ns = {}
        local_ns['repl'] = self
        local_ns.update(self.types.biz)
        local_ns.update({p.name: p for p in self.proxies})
        local_ns.update(namespace or {})

        # enter an ipython shell
        self.shell.mainloop(local_ns=local_ns)

    @property
    def functions(self) -> List[Text]:
        """
        Get list of names of all registered functions in the REPL.
        """
        return sorted(p.target.__name__ for p in self.proxies)


class Function(RegistryProxy):
    def __init__(self, func, decorator):
        super().__init__(func, decorator)

    @property
    def source(self) -> Text:
        print(inspect.getsource(self.target))
