from typing import Text
from uuid import UUID

from ravel.entity import Entity
from ravel.constants import (
    IS_RESOURCE,
    IS_BATCH,
    ID,
)


def is_resource(obj):
    """
    Return True if obj is an instance of Resource.
    """
    return (
        isinstance(obj, Entity)
        and getattr(obj, IS_RESOURCE, False)
    )


def is_batch(obj) -> bool:
    """
    Return True if obj is an instance of Resource.
    """
    return (
        isinstance(obj, Entity)
        and getattr(obj, IS_BATCH, False)
    )


def is_resource_type(obj):
    return (
        isinstance(obj, type)
        and getattr(obj, IS_RESOURCE, False)
    )


def is_batch_type(obj):
    return (
        isinstance(obj, type)
        and getattr(obj, IS_BATCH, False)
    )


def repr_res_id(resource: 'Resource') -> Text:
    """
    Return a string version of a Resource ID.
    """
    # TODO: rename this function
    if resource is None:
        return 'None'

    _id = resource[ID]

    if _id is None:
        return '?'
    elif isinstance(_id, str):
        return _id
    elif isinstance(_id, UUID):
        return _id.hex
    else:
        return repr(_id)
