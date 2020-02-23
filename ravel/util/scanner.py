import os
import inspect
import importlib

from os.path import splitext


class Scanner:
    def __init__(self, context=None):
        self.context = context or {}

    def scan(self, package_name, context=None):
        # TODO: allow package_name to refer to a module rather than a package
        context = context if context is not None else self.context
        package_init_module = importlib.import_module(package_name)
        package_dir = os.path.split(package_init_module.__file__)[0]
        n = package_name.count('.') + 1
        package_parent_dir = '/' + '/'.join(
            package_dir.strip('/').split('/')[:-n]
        )

        for dir_name, sub_dirs, file_names in os.walk(package_dir):
            if '__init__.py' in file_names:
                n = len(package_parent_dir)
                pkg_path = dir_name[n + 1:].replace("/", ".")
                for file_name in file_names:
                    if file_name.endswith('.py'):
                        mod_path = f'{pkg_path}.{splitext(file_name)[0]}'
                        module = importlib.import_module(mod_path)
                        self.scan_module(module, context)
        return context

    def scan_module(self, module, context):
        for k, v in inspect.getmembers(module, predicate=self.predicate):
            try:
                self.callback(k, v, context)
            except Exception as exc:
                self.on_error(exc, module, context, k, v)

    def predicate(self, value) -> bool:
        return True

    def callback(self, name, value, context):
        pass

    def on_error(self, exc, module, context, name, value):
        raise exc
