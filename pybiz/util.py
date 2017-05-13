from .const import IS_BIZOBJ_ANNOTATION


def is_bizobj(obj):
    """
    Return True if obj is an instance of BizObject.
    """
    return hasattr(obj, IS_BIZOBJ_ANNOTATION) if obj else False
