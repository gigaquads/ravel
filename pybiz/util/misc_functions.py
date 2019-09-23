from uuid import UUID
from importlib import import_module
from typing import (
    List, Dict, ForwardRef, Text, Tuple, Set, Type,
    _GenericAlias as GenericAlias
)

from copy import deepcopy

from pybiz.constants import (
    IS_BIZ_OBJECT_ANNOTATION,
    IS_BIZ_LIST_ANNOTATION,
)

_dict_keys = {}.keys().__class__
_dict_values = {}.values().__class__


def is_biz_obj(obj):
    """
    Return True if obj is an instance of BizObject.
    """
    return (
        getattr(obj, IS_BIZ_OBJECT_ANNOTATION, False)
        if obj else False
    )


def is_biz_list(obj) -> bool:
    """
    Return True if obj is an instance of BizObject.
    """
    return (
        getattr(obj, IS_BIZ_LIST_ANNOTATION, False)
        if obj is not None else False
    )


def is_sequence(obj) -> bool:
    """
    Return True if obj is a generic sequence type, like a list or tuple.
    """
    return isinstance(obj, (list, tuple, set, _dict_keys, _dict_values))


def repr_biz_id(biz_obj: 'BizObject') -> Text:
    """
    Return a string version of a BizObject ID.
    """
    if biz_obj is None:
        return 'None'
    _id = biz_obj['_id']
    if _id is None:
        return '?'
    elif isinstance(_id, str):
        return _id
    elif isinstance(_id, UUID):
        return _id.hex
    else:
        return repr(_id)


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
    if isinstance(obj, type):
        return obj.__name__
    else:
        return obj.__class__.__name__


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


def extract_biz_info_from_annotation(annotation) -> Tuple[bool, Text]:
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
            key = extract_biz_info_from_annotation(arg)[1]
            many = True

    return (many, key)


def flatten_sequence(target) -> Set:
    flattened = set()
    for child_target in target:
        if is_sequence(child_target):
            flattened.update(flatten_sequence(child_target))
        else:
            flattened.add(child_target)
    return flattened
