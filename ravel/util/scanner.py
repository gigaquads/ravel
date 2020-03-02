import re
import os
import inspect
import importlib

from os.path import splitext

from appyratus.files.json import Json

from ravel.util.loggers import console


class Scanner:
    def __init__(self, context=None):
        self.context = context or {}

    def scan(self, package_name, context=None):
        # TODO: allow package_name to refer to a module rather than a package
        context = context if context is not None else self.context
        root_module = importlib.import_module(package_name)
        root_filename = os.path.basename(root_module.__file__)
        if root_filename != '__init__.py':
            self.scan_module(root_module, context)
        else:
            package_dir = os.path.split(root_module.__file__)[0]
            if re.match(f'\./', package_dir):
                # ensure we use an absolute path for the package dir
                # to prevent strange string truncation results below
                package_dir = os.path.realpath(package_dir)
            package_path_len = package_name.count('.') + 1
            package_parent_dir = '/' + '/'.join(
                package_dir.strip('/').split('/')[:-package_path_len]
            )
            for dir_name, sub_dirs, file_names in os.walk(package_dir):
                file_names = set(file_names)
                if '.ravel' in file_names:
                    dot_file_path = os.path.join(dir_name, '.ravel')
                    dot_data = Json.read(dot_file_path) or {}
                    ignore = dot_data.get('scanner', {}).get('ignore', False)
                    if ignore:
                        console.debug(f'scanner ignoring {dir_name}')
                        continue
                if '__init__.py' in file_names:
                    dir_name_offset = len(package_parent_dir)
                    pkg_path = dir_name[dir_name_offset + 1:].replace("/", ".")
                    for file_name in file_names:
                        if file_name.endswith('.py'):
                            mod_path = f'{pkg_path}.{splitext(file_name)[0]}'
                            try:
                                module = importlib.import_module(mod_path)
                            except Exception as exc:
                                self.on_import_error(exc, mod_path, context)
                                continue
                            self.scan_module(module, context)
        return context

    def scan_module(self, module, context):
        for k, v in inspect.getmembers(module, predicate=self.predicate):
            try:
                self.on_match(k, v, context)
            except Exception as exc:
                self.on_match_error(exc, module, context, k, v)

    def predicate(self, value) -> bool:
        return True

    def on_match(self, name, value, context):
        pass

    def on_import_error(self, exc, module_path, context):
        raise exc

    def on_match_error(self, exc, module, context, name, value):
        raise exc
