import sys

from typing import Text, Dict, List

from appyratus.cli import CliProgram, PositionalArg


class ApplicationRouter(CliProgram):
    """
    # Application Router
    Program convenience for routing commands to multiple
    applications through a single command-line interface
    """

    def __init__(
        self,
        manifest: 'Manifest' = None,
        applications: List = None,
        *args,
        **kwargs
    ):
        """
        # Init
        Do not merge unknown args into the args dict, as the app router
        only cares about the app field and nothing else.
        """
        super().__init__(merge_unknown=False, *args, **kwargs)
        self._manifest = manifest
        self._applications = applications

    def args(self):
        """
        # Args
        A list of arguments in use by the app router.  The first
        argument being the app that the CLI request will be
        routed to
        """
        return [
            PositionalArg(name='app', usage='the Application to utilize')
        ]

    def perform(self, program: 'CliProgram'):
        """
        # Perform routing
        Route to the app provided in the CLI's first argument
        """
        app_name = self.cli_args.app
        app = self.get_app(app_name)
        if not app:
            raise Exception(f'Unable to locate app "{app_name}"')
        app_method = getattr(self, f'run_{app_name}', None)
        if callable(app_method):
            return app_method(app)
        else:
            self.app_lifecycle(app=app)

    def get_app(self, name: Text):
        """
        # Get app
        First by applications dictionary (provided when initialized)
        And if not there, then an attribute on this your router class
        """
        if not self._applications:
            app_dict = {}
        else:
            app_dict = self._applications.get(name)
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
        app.bootstrap(
            manifest=manifest or self._manifest, **(bootstrap_kwargs or {})
        )
        app.start(**(start_kwargs or {}))
        return app

    def run_cli(self, cli_app: 'CliApplication'):
        """
        # Run Cli
        A special implementation for the CLI app to provide this program's
        unknown CLI args to the CLI app program.
        """
        return self.app_lifecycle(
            app=cli_app,
            bootstrap_kwargs={'cli_args': self._unknown_cli_args}
        )
