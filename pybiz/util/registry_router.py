import sys

from typing import Text, Dict, List

from appyratus.cli import CliProgram, PositionalArg


class RegistryRouter(CliProgram):
    """
    # Registry Router
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
        Do not merge unknown args into the args dict, as the registry router
        only cares about the registry field and nothing else.
        """
        super().__init__(merge_unknown=False, *args, **kwargs)
        self._manifest = manifest
        self._registries = registries

    def args(self):
        """
        # Args
        A list of arguments in use by the registry router.  The first
        argument being the registry that the CLI request will be
        routed to
        """
        return [
            PositionalArg(name='registry', usage='the registry to utilize')
        ]

    def perform(self, program: 'CliProgram'):
        """
        # Perform routing
        Route to the registry provided in the CLI's first argument
        """
        registry_name = self.cli_args.registry
        registry = self.get_registry(registry_name)
        if not registry:
            raise Exception(f'Unable to locate registry "{registry_name}"')
        registry_method = getattr(self, f'run_{registry_name}', None)
        if callable(registry_method):
            return registry_method(registry)
        else:
            self.registry_lifecycle(registry=registry)

    def get_registry(self, name: Text):
        """
        # Get registry
        First by registries dictionary (provided when initialized)
        And if not there, then an attribute on this your router class
        """
        if not self._registries:
            registry_dict = {}
        else:
            registry_dict = self._registries.get(name)
        registry_attr = getattr(self, name, None)
        if registry_dict:
            return registry_dict
        elif registry_attr:
            return registry_attr()

    def registry_lifecycle(
        self,
        registry: 'Registry',
        manifest: Text = None,
        bootstrap_kwargs: Dict = None,
        start_kwargs: Dict = None
    ):
        """
        Registry lifecycle
        Perform necessary bootstrapping with manifest and then fire it up
        """
        registry.bootstrap(
            manifest=manifest or self._manifest, **(bootstrap_kwargs or {})
        )
        registry.start(**(start_kwargs or {}))
        return registry

    def run_cli(self, cli_registry: 'CliRegistry'):
        """
        # Run Cli
        A special implementation for the cli registry to provide this program's
        unknown cli args to the cli registry program.
        """
        return self.registry_lifecycle(
            registry=cli_registry,
            bootstrap_kwargs={'cli_args': self._unknown_cli_args}
        )
