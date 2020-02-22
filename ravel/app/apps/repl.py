from typing import Dict, List, Text, Type

from IPython.terminal.embed import InteractiveShellEmbed

from ravel.app.base import Application, Endpoint


class Repl(Application):
    """
    Repl is a Application that collects all registered functions and
    injects them into an interactive Python shell, or REPL. This is useful for
    experimenting with an API from a command-line interface.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.shell = InteractiveShellEmbed()
        self._namespace = {}

    @property
    def endpoint_type(self) -> Type['ReplFunction']:
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
        return sorted(self.endpoints.keys())

    def on_start(self):
        """
        Start a new REPL with all registered functions available in the REPL
        namespace.
        """
        # build the shell namespace
        local_ns = {}
        local_ns['repl'] = self

        local_ns.update(self._namespace)
        local_ns.update(self.api)
        local_ns.update(self.res)
        local_ns.update(self.dal)

        # enter an ipython shell
        self.shell.mainloop(local_ns=local_ns)

    def on_response(
        self,
        repl_function: 'ReplFunction',
        result,
        *args,
        **kwargs
    ) -> object:
        if repl_function.memoized:
            repl_function.memoize(result)
        return super().on_response(repl_function, result, *args, **kwargs)


class ReplFunction(Endpoint):
    def __init__(self, func, decorator):
        super().__init__(func, decorator)
        self.return_values = []

    @property
    def memoized(self) -> bool:
        return self.decorator.kwargs.get('memoized', False)

    def memoize(self, value):
        self.return_values.append(value)
