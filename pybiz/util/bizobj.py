from copy import deepcopy
from importlib import import_module
from typing import (
    Dict,
    List,
    Set,
    Text,
)
from uuid import UUID

from pybiz.constants import (
    IS_BIZLIST_ANNOTATION,
    IS_BIZOBJ_ANNOTATION,
)

_dict_keys = {}.keys().__class__
_dict_values = {}.values().__class__


def is_bizobj(obj):
    """
    Return True if obj is an instance of BizObject.
    """
    return getattr(obj, IS_BIZOBJ_ANNOTATION, False) if obj else False


def is_bizlist(obj) -> bool:
    """
    Return True if obj is an instance of BizObject.
    """
    return getattr(obj, IS_BIZLIST_ANNOTATION, False) if obj is not None else False


def is_sequence(obj) -> bool:
    """
    Return True if obj is a generic sequence type, like a list or tuple.
    """
    return isinstance(obj, (list, tuple, set, _dict_keys, _dict_values))


def repr_biz_id(bizobj: 'BizObject') -> Text:
    """
    Return a string version of a BizObject ID.
    """
    _id = bizobj['_id']
    if _id is None:
        return '?'
    elif isinstance(_id, str):
        return _id[:7]
    elif isinstance(_id, UUID):
        return _id.hex[:7]
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


def remove_keys(records: List[Dict], keys: Set[Text],
                in_place=True) -> List[Dict]:
    """
    Remove keys from a dict. Non-recursive.
    """
    records_out = []
    for record in records:
        if not in_place:
            record = deepcopy(record)
        for k in keys:
            record.pop(k, None)
        records_out.append(record)
    return records_out
