from uuid import UUID

from pybiz.constants import (
    IS_BIZ_OBJECT_ANNOTATION,
    IS_BIZ_LIST_ANNOTATION,
    ID_FIELD_NAME,
)


def is_biz_object(obj):
    """
    Return True if obj is an instance of BizObject.
    """
    return (
        getattr(obj, '_pybiz_is_biz_object', False)
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


def repr_biz_id(biz_obj: 'BizObject') -> Text:
    """
    Return a string version of a BizObject ID.
    """
    if biz_obj is None:
        return 'None'
    _id = biz_obj[ID_FIELD_NAME]
    if _id is None:
        return '?'
    elif isinstance(_id, str):
        return _id
    elif isinstance(_id, UUID):
        return _id.hex
    else:
        return repr(_id)
