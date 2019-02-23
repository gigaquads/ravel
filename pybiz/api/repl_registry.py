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
        self._namespace = {}

    @property
    def proxy_type(self):
        return ReplFunction

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
                    'foo': Foo().save(),
                    'bar': Bar().save()
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

    def on_decorate(self, proxy: 'ReplFunction'):
        pass

    def on_bootstrap(self, *args, **kwargs):
        pass

    def on_start(self):
        """
        Start a new REPL with all registered functions available in the REPL
        namespace.
        """
        # build the shell namespace
        local_ns = {}
        local_ns['repl'] = self

        local_ns.update(self._namespace)
        local_ns.update(dict(self.proxies))
        local_ns.update(self.types.biz)
        local_ns.update(self.types.dao)

        # enter an ipython shell
        self.shell.mainloop(local_ns=local_ns)

    def on_request(self, proxy: 'ReplFunction', *args, **kwargs):
        return (args, kwargs)

    def on_response(self, proxy: 'ReplFunction', result, *args, **kwargs):
        return super().on_response(proxy, result, *args, **kwargs)


class ReplFunction(RegistryProxy):
    def __init__(self, func, decorator):
        super().__init__(func, decorator)

    @property
    def source(self) -> None:
        print(inspect.getsource(self.target))
