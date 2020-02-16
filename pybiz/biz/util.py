from typing import Text
from uuid import UUID

from pybiz.biz.entity import Entity
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
        isinstance(obj, Entity)
        and getattr(obj, IS_BIZ_OBJECT_ANNOTATION, False)
    )


def is_batch(obj) -> bool:
    """
    Return True if obj is an instance of Resource.
    """
    return (
        isinstance(obj, Entity)
        and getattr(obj, IS_BIZ_LIST_ANNOTATION, False)
    )


def is_resource_type(obj):
    return (
        isinstance(obj, type)
        and getattr(obj, IS_BIZ_OBJECT_ANNOTATION, False)
    )


def is_batch_type(obj):
    return (
        isinstance(obj, type)
        and getattr(obj, IS_BIZ_LIST_ANNOTATION, False)
    )


def repr_biz_id(resource: 'Resource') -> Text:
    """
    Return a string version of a Resource ID.
    """
    # TODO: rename this function
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
