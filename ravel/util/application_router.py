import sys

from typing import Text, Dict, List

from appyratus.cli import CliProgram, PositionalArg, OptionalArg

from ravel.util.misc_functions import import_object


class ApplicationRouter(CliProgram):
    """
    # Application Router
    Program convenience for routing commands to multiple
    applications through a single command-line interface
    """

    def __init__(
        self,
        manifest: 'Manifest' = None,
        applications: Dict = None,
        default_app: Text = None,
        *args,
        **kwargs
    ):
        """
        # Init
        Do not merge unknown args into the args dict, as the app router
        only cares about the app field and nothing else.
        """
        self._manifest = manifest
        self._default_app = default_app
        self._apps = applications or {}

        # if applications were specified by dotted-path,
        # we perform the import here dynamically.
        for k, v in self._apps.items():
            if isinstance(v, str):
                self._apps[k] = import_object(v)
        super().__init__(merge_unknown=False, *args, **kwargs)

    def args(self):
        """
        # Args
        A list of arguments in use by the app router.  The first
        argument being the app that the CLI request will be
        routed to
        """
        app_names = ', '.join([r for r in self._apps.keys()])
        app_arg_args = {'name': 'app', 'usage': f'the app to utilize [{app_names}]'}
        if self._default_app:
            # default app is provided, so we will not require the app positional key
            app_arg = OptionalArg(**app_arg_args, default=self._default_app)
        else:
            app_arg = PositionalArg(**app_arg_args)
        return [app_arg]

    @staticmethod
    def perform(program):
        """
        # Perform routing
        Route to the app provided in the CLI's first argument
        """
        app_name = program.cli_args.app
        app = program.get_app(app_name)
        if not app:
            raise Exception(f'Unable to locate app "{app_name}"')
        app_method = getattr(program, f'run_{app_name}', None)
        if callable(app_method):
            return app_method(app)
        else:
            program.app_lifecycle(app=app)

    def get_app(self, name: Text):
        """
        # Get app
        First by applications dictionary (provided when initialized)
        And if not there, then an attribute on this your router class
        """
        if not self._apps:
            app_dict = {}
        else:
            app_dict = self._apps.get(name)
        app_attr = getattr(self, name, None)
        if app_dict:
            return app_dict
        elif app_attr:
            return app_attr()

    def app_lifecycle(
        self,
        app: 'Application',
        manifest: Text = None,
        bootstrap_kwargs: Dict = None,
        start_kwargs: Dict = None
    ):
        """
        Application lifecycle
        Perform necessary bootstrapping with manifest and then fire it up
        """
        app.bootstrap(manifest=manifest or self._manifest, **(bootstrap_kwargs or {}))
        app.start(**(start_kwargs or {}))
        return app

    def run_cli(self, cli_app: 'CliApplication'):
        """
        # Run Cli
        A special implementation for the CLI app to provide this program's
        unknown CLI args to the CLI app program.
        """
        return self.app_lifecycle(
            app=cli_app, bootstrap_kwargs={'cli_args': self._unknown_cli_args}
        )
