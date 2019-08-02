import sys

from typing import Text, Dict, List

from appyratus.cli import CliProgram, PositionalArg


class ApiRouter(CliProgram):
    """
    # Api Router
    Program convenience for routing commands to multiple
    registries through a single command-line interface
    """

    def __init__(
        self,
        manifest: 'Manifest' = None,
        registries: List = None,
        *args,
        **kwargs
    ):
        """
        # Init
        Do not merge unknown args into the args dict, as the api router
        only cares about the api field and nothing else.
        """
        super().__init__(merge_unknown=False, *args, **kwargs)
        self._manifest = manifest
        self._registries = registries

    def args(self):
        """
        # Args
        A list of arguments in use by the api router.  The first
        argument being the api that the CLI request will be
        routed to
        """
        return [
            PositionalArg(name='api', usage='the api to utilize')
        ]

    def perform(self, program: 'CliProgram'):
        """
        # Perform routing
        Route to the api provided in the CLI's first argument
        """
        api_name = self.cli_args.api
        api = self.get_api(api_name)
        if not api:
            raise Exception(f'Unable to locate api "{api_name}"')
        api_method = getattr(self, f'run_{api_name}', None)
        if callable(api_method):
            return api_method(api)
        else:
            self.api_lifecycle(api=api)

    def get_api(self, name: Text):
        """
        # Get api
        First by registries dictionary (provided when initialized)
        And if not there, then an attribute on this your router class
        """
        if not self._registries:
            api_dict = {}
        else:
            api_dict = self._registries.get(name)
        api_attr = getattr(self, name, None)
        if api_dict:
            return api_dict
        elif api_attr:
            return api_attr()

    def api_lifecycle(
        self,
        api: 'Api',
        manifest: Text = None,
        bootstrap_kwargs: Dict = None,
        start_kwargs: Dict = None
    ):
        """
        Api lifecycle
        Perform necessary bootstrapping with manifest and then fire it up
        """
        api.bootstrap(
            manifest=manifest or self._manifest, **(bootstrap_kwargs or {})
        )
        api.start(**(start_kwargs or {}))
        return api

    def run_cli(self, cli_api: 'CliApi'):
        """
        # Run Cli
        A special implementation for the cli api to provide this program's
        unknown cli args to the cli api program.
        """
        return self.api_lifecycle(
            api=cli_api,
            bootstrap_kwargs={'cli_args': self._unknown_cli_args}
        )
