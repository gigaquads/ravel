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

    def on_request(self, proxy: 'Function', *args, **kwargs):
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
        local_ns.update(dict(self.proxies))
        local_ns.update(namespace or {})

        # enter an ipython shell
        self.shell.mainloop(local_ns=local_ns)

    @property
    def namespace(self) -> Dict:
        """
        iPython's embedded shell session namespace dict. Update this dict from
        methods when you want to, say, rerun and reload fixture data inside a
        REPL, like:

        ```python3
            @repl()
            def reset_fixtures():
                fixtures = {
                    'foo': Foo.create().save(),
                    'bar': Bar.create().save()
                }
                repl.namespace.update(fixtures)
                return fixtures
            ```

        Now, inside the REPL session, you can do `reset_fixtures()` to reset the
        global variables available to you in the shell.
        """
        return self.shell.user_ns

    @property
    def functions(self) -> List[Text]:
        """
        Get list of names of all registered functions in the REPL.
        """
        return sorted(self.proxies.keys())


class Function(RegistryProxy):
    def __init__(self, func, decorator):
        super().__init__(func, decorator)

    @property
    def source(self) -> Text:
        print(inspect.getsource(self.target))
