import sys

from typing import Text, Dict

from appyratus.cli import CliProgram, PositionalArg


class RegistryRouter(CliProgram):
    """
    # Registry Router
    Program convenience for routing commands to multiple
    registries through a single command-line interface
    """

    def __init__(self, manifest: 'Manifest' = None, *args, **kwargs):
        """
        # Init
        Do not merge unknown args into the args dict, as the registry router
        only cares about the registry field and nothing else.
        """
        super().__init__(merge_unknown=False, *args, **kwargs)
        self._manifest = manifest

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

    def perform(self, program):
        """
        # Perform routing
        Route to the registry provided in the CLI's first argument
        """
        registry_name = self.cli_args.registry
        if not hasattr(self, registry_name):
            raise Exception('Unknown registry "{}"'.format(registry_name))
        registry = getattr(self, registry_name)()

    def run_registry(
        self,
        registry: 'Registry',
        manifest: Text = None,
        bootstrap_kwargs: Dict = None,
        start_kwargs: Dict = None
    ):
        registry.bootstrap(
            manifest=manifest or self._manifest, **(bootstrap_kwargs or {})
        )
        registry.start(**(start_kwargs or {}))
        return registry

    def run_cli(self, cli_registry: 'CliRegistry'):
        return self.run_registry(
            registry=cli_registry,
            bootstrap_kwargs={'cli_args': self._unknown_cli_args}
        )
