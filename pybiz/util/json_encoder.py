from appyratus.json import JsonEncoder as BaseJsonEncoder

from .bizobj import is_bizobj, is_bizlist


class JsonEncoder(BaseJsonEncoder):
    def default(self, target):
        if is_bizobj(target) or is_bizobj(target):
            return target.dump()
        else:
            return super().default(target)
