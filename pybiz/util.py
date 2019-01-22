from appyratus.json import JsonEncoder as BaseJsonEncoder

from pybiz.constants import IS_BIZOBJ_ANNOTATION


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
