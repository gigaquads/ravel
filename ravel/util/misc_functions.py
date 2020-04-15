import inspect
import socket

from uuid import UUID
from importlib import import_module
from types import GeneratorType
from typing import (
    List, Dict, ForwardRef, Text, Tuple, Set, Type,
    _GenericAlias as GenericAlias, Callable
)

from copy import deepcopy


DictKeySet = type({}.keys())
DictValueSet = type({}.values())


def is_sequence(obj) -> bool:
    """
    Return True if obj is a generic sequence type, like a list or tuple.
    """
    return isinstance(obj, (list, tuple, set, DictKeySet, DictValueSet))


def import_object(dotted_path: Text) -> object:
    """
    Import an object from a module, given a dotted path to it.
    """
    obj_path = dotted_path.split('.')

    if len(obj_path) < 2:
        raise ImportError(dotted_path)

    module_path_str = '.'.join(obj_path[:-1])
    obj_name = obj_path[-1]

    try:
        module = import_module(module_path_str)
        obj = getattr(module, obj_name)
    except Exception:
        raise ImportError(f'failed to import {dotted_path}')

    return obj


def get_class_name(obj):
    if not obj:
        return None
    if isinstance(obj, type):
        return obj.__name__
    else:
        return obj.__class__.__name__


def get_callable_name(obj):
    if inspect.isfunction(obj):
        return obj.__name__
    elif inspect.ismethod(obj):
        return obj.__func__.__name__
    else:
        raise ValueError(str(obj))


def remove_keys(
    records: List[Dict], keys: Set[Text], in_place=True
) -> List[Dict]:
    """
    Remove keys from a dict. Non-recursive.
    """
    records_out = []
    for record in records:
        if record is not None:
            if not in_place:
                record = deepcopy(record)
            for k in keys:
                record.pop(k, None)
        records_out.append(record)
    return records_out


def normalize_to_tuple(obj):
    """
    If obj is a tuple, return it as-is; otherwise, return it in a tuple with it
    as its single element.
    """
    if obj is not None:
        if isinstance(obj, (list, tuple)):
            return tuple(obj)
        return (obj, )
    return tuple()


def extract_res_info_from_annotation(annotation) -> Tuple[bool, Text]:
    """
    Return a tuple of metadata pertaining to `obj`, which is some object
    used in a type annotation, passed in by the caller.
    """
    key = None
    many = False

    if isinstance(annotation, str):
        key = annotation.split('.')[-1]
    elif isinstance(annotation, type):
        key = annotation.__name__.split('.')[-1]
    elif isinstance(annotation, ForwardRef):
        key = annotation.__forward_arg__
    elif (
        (isinstance(annotation, GenericAlias)) and
        (annotation._name in {'List', 'Tuple', 'Set'})
    ):
        if annotation.__args__:
            arg = annotation.__args__[0]
            key = extract_res_info_from_annotation(arg)[1]
            many = True

    if key == '_empty':
        key = None

    return (many, key)


def flatten_sequence(seq) -> List:
    flattened = []
    if not seq:
        return flattened
    for obj in seq:
        if is_sequence(obj):
            flattened.extend(flatten_sequence(obj))
        elif isinstance(obj, GeneratorType):
            flattened.extend(list(obj))
        elif isinstance(obj, dict):
            flattened.extend(obj.values())
        else:
            flattened.append(obj)
    return flattened


def inject(func: Callable, data: Dict):
    """
    Dynamically inject data dict into a function's lexcial scope.
    """
    func.__globals__.update(data)


def union(sequences):
    if sequences:
        if len(sequences) == 1:
            return sequences[0]
        else:
            return set.union(*sequences)
    else:
        return set()


def is_port_in_use(addr: Text) -> bool:
    """
    Utility method for determining if the server address is already in use.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        host, port_str = addr.split(':')
        sock.bind((host, int(port_str)))
        return False
    except OSError as err:
        if err.errno == 48:
            return True
        else:
            raise err
    finally:
        sock.close()


