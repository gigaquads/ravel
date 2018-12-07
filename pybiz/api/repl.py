import inspect

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
        return ReplRegistryProxy

    def on_decorate(self, proxy):
        pass

    def on_request(self, proxy, signature, *args, **kwargs):
        return (args, kwargs)

    def start(self, namespace=None, *args, **kwargs):
        """
        Start a new REPL with all registered functions available in the REPL
        namespace.
        """
        self.shell.mainloop(
            local_ns=self._build_shell_namespace(namespace or {})
        )

    def _build_shell_namespace(self, custom_namespace):
        ns = {}
        ns['repl'] = self
        ns.update(self.biz_types.to_dict())
        ns.update({p.name: p for p in self.proxies})
        ns.update(custom_namespace)
        return ns

    @property
    def functions(self):
        """
        Get list of names of all registered functions in the REPL.
        """
        return sorted(p.target.__name__ for p in self.proxies)


class ReplRegistryProxy(RegistryProxy):
    def __init__(self, func, decorator):
        super().__init__(func, decorator)

    def debug(self, *args, **kwargs):
        return self.call_target(args, kwargs, pybiz_debug=True)

    @property
    def source(self):
        print(inspect.getsource(self.target))
