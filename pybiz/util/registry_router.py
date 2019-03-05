import sys

from appyratus.cli import CliProgram, PositionalArg


class RegistryRouter(CliProgram):
    """
    # Registry Router
    Program convenience for routing commands to multiple
    registries through a single command-line interface
    """

    def __init__(self, *args, **kwargs):
        """
        # Init
        Do not merge unknown args into the args dict, as the registry router
        only cares about the registry field and nothing else.
        """
        super().__init__(merge_unknown=False, *args, **kwargs)

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
        del sys.argv[1]
        if not hasattr(self, registry_name):
            raise Exception('Unknown registry "{}"'.format(registry_name))
        registry = getattr(self, registry_name)()

