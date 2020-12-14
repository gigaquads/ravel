import traceback

from typing import Text, Dict, Type, Union, Set

from ravel.util.loggers import console
from ravel.util.scanner import Scanner


class ManifestScanner(Scanner):
    """
    The manifest's type scanner is used to detect Store and Resource classes.
    """

    def __init__(self, manifest: 'Manifest', *args, **kwargs):
        super().__init__(*args, **kwargs)

        from ravel.store.base.store import Store
        from ravel.resource import Resource

        self.manifest = manifest
        self.base_store_class = Store
        self.base_resource_class = Resource

        self.context.store_classes = {}
        self.context.resource_classes = {}

    def predicate(self, value) -> bool:
        """
        Predicate should evaluate True for any object we want this scanner to
        match and pass on to the self.on_match callback.
        """
        def is_resource_or_store_class(value) -> bool:
            return (
                isinstance(value, type)
                and issubclass(value, (
                    self.base_resource_class,
                    self.base_store_class
                ))
            )

        return (
            is_resource_or_store_class(value)
        )

    def on_match(self, name: Text, value, context):
        """
        Add the class object to the appripriate container.
        """
        if isinstance(value, type):
            if issubclass(value, self.base_resource_class):
                if not value.ravel.is_abstract:
                    context.resource_classes[name] = value
            elif issubclass(value, self.base_store_class):
                context.store_classes[name] = value

    def on_import_error(self, exc: Exception, module_name: Text, context):
        """
        If the scanner fails to import a module while it walks the file
        system, it comes here to handle and report the problem.
        """
        exc_str = traceback.format_exc()
        console.error(
            message=f'manifest could not scan module: {module_name}',
            data={'traceback': exc_str.split('\n')}
        )

    def on_match_error(self, exc: Exception, module, context, name, value):
        """
        If there's a problem in self.on_match, we come here.
        """
        exc_str = traceback.format_exc()
        console.error(
            message=f'error while scanning {name} ({type(value)})',
            data={'traceback': exc_str.split('\n')}
        )

