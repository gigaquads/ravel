import sys

from appyratus.cli import CliProgram, PositionalArg


class RegistryRouter(CliProgram):
    """
    # Registry Router
    Program convenience for routing to different registries
    """

    def args(self):
        """
        # Args
        A list of arguments in use by the registry router.  The first
        argument being the registry that the CLI request will be
        routed to
        """
        return [PositionalArg(name='registry')]

    def perform(self):
        """
        # Perform routing
        Route to the registry provided in the CLI's first argument
        """
        registry_name = self.cli_args.registry
        del sys.argv[1]
        if not hasattr(self, registry_name):
            raise Exception('Unknown registry "{}"'.format(registry_name))
        registry = getattr(self, registry_name)()
