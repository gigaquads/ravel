from typing import Text
from uuid import UUID

from pybiz.constants import (
    IS_BIZ_OBJECT_ANNOTATION,
    IS_BIZ_LIST_ANNOTATION,
    ID_FIELD_NAME,
)


def is_resource(obj):
    """
    Return True if obj is an instance of Resource.
    """
    return (
        getattr(obj, IS_BIZ_OBJECT_ANNOTATION, False)
        if obj else False
    )


def is_batch(obj) -> bool:
    """
    Return True if obj is an instance of Resource.
    """
    return (
        getattr(obj, IS_BIZ_LIST_ANNOTATION, False)
        if obj is not None else False
    )


def is_biz_class(obj):
    return isinstance(obj, type) and is_resource(obj)


def repr_biz_id(resource: 'Resource') -> Text:
    """
    Return a string version of a Resource ID.
    """
    if resource is None:
        return 'None'
    _id = resource[ID_FIELD_NAME]
    if _id is None:
        return '?'
    elif isinstance(_id, str):
        return _id
    elif isinstance(_id, UUID):
        return _id.hex
    else:
        return repr(_id)
