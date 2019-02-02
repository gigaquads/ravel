from uuid import UUID
from importlib import import_module

from appyratus.json import JsonEncoder as BaseJsonEncoder

from pybiz.constants import IS_BIZOBJ_ANNOTATION


dict_keys = {}.keys().__class__
dict_values = {}.values().__class__


class JsonEncoder(BaseJsonEncoder):
    def default(self, target):
        if is_bizobj(target):
            return target.dump()
        else:
            return super().default(target)


def is_bizobj(obj):
    """
    Return True if obj is an instance of BizObject.
    """
    return getattr(obj, IS_BIZOBJ_ANNOTATION, False) if obj else False


def is_sequence(obj):
    return isinstance(obj, (list, tuple, set, dict_keys, dict_values))


def ensure(*conditions):
    for cond in conditions:
        assert cond


def repr_id(bizobj):
    _id = bizobj['_id']
    if _id is None:
        return '?'
    elif isinstance(_id, str):
        return _id[:7]
    elif isinstance(_id, UUID):
        return _id.hex[:7]
    else:
        return repr(_id)


def import_object(dotted_path):
    obj_path = dotted_path.split('.')
    assert len(obj_path) > 1

    module_path_str = '.'.join(obj_path[:-1])
    obj_name = obj_path[-1]

    try:
        module = import_module(module_path_str)
        obj = getattr(module, obj_name)
    except Exception:
        raise ImportError(
            'failed to import object {}'.format(obj_name)
        )
    return obj
