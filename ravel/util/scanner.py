import re
import os
import inspect
import importlib

from typing import Dict, Callable
from os.path import splitext

from appyratus.files.json import Json
from appyratus.utils.dict_utils import DictObject

from ravel.logging import ConsoleLoggerInterface

# TODO: allow package_name to refer to a module rather than a package

class Scanner:
    """
    The Scanner recursively walks the filesystem, rooted at a Python package
    or module filepath, and matches each object contained in each Python
    source file against a logical predicate. If the predicate matches, the
    object is passed into the overriden on_match instance method.
    """

    def __init__(
        self,
        predicate: Callable = None,
        callback: Callable = None,
    ):
        self.log = ConsoleLoggerInterface('scanner')
        self.context = DictObject()
        if predicate:
            self.predicate = predicate
        if callback:
            self.on_match = callback

    def scan(self, package_name, context: Dict = None, verbose=False):
        context = dict(self.context.to_dict(), **(context or {}))
        context = DictObject(context)

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
                        self.log.debug(f'scanner ignoring {dir_name}')
                        sub_dirs.clear()
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

        self.context = context
        return context

    def scan_module(self, module, context):
        if None in module.__dict__:
            # XXX: why is this happenings?
            del module.__dict__[None]
        for k, v in inspect.getmembers(module, predicate=self.predicate):
            # if verbose:
            #     console.debug(
            #         f'scanner matched "{k}" '
            #         f'{str(v)[:40] + "..." if len(str(v)) > 40 else v}'
            #     )
            try:
                self.on_match(k, v, context)
            except Exception as exc:
                self.on_match_error(exc, module, context, k, v)

    def predicate(self, value) -> bool:
        return True

    def on_match(self, name, value, context):
        context[name] = value

    def on_import_error(self, exc, module_path, context):
        self.log.exception(
            message='scanner encountered an import error',
            data={
                'module': module_path,
            }
        )

    def on_match_error(self, exc, module, context, name, value):
        self.log.exception(
            message=f'scanner encountered an error while scanning object',
            data={
                'module': module.__name__,
                'object': name,
                'type': type(value),
            }
        )
